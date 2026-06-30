namespace NServiceBus;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

static class NonDurableTransportTracing
{
    public const string ActivitySourceName = "NServiceBus.Transport.NonDurable";

    public const string SendActivityName = "NServiceBus.Transport.NonDurable.Send";
    public const string ScheduleActivityName = "NServiceBus.Transport.NonDurable.Schedule";
    public const string ProcessActivityName = "NServiceBus.Transport.NonDurable.Process";

    const string MessagingSystem = "messaging.system";
    const string DestinationName = "messaging.destination.name";
    const string OperationName = "messaging.operation.name";
    const string OperationType = "messaging.operation.type";
    const string MessageId = "messaging.message.id";
    const string ConversationId = "messaging.message.conversation_id";
    const string ErrorType = "error.type";
    const string NonDurableSystemName = "nondurable";
    const string EnqueuedEventName = "nondurable.enqueued";
    const string ScheduledEventName = "nondurable.scheduled";
    const string HandoffEventName = "nondurable.handoff";

    static readonly ActivitySource activitySource = new(ActivitySourceName, "0.1.0");

    public static bool HasListeners() => activitySource.HasListeners();

    public static Activity? StartSend(string destination, string messageId, IReadOnlyDictionary<string, string> headers, bool delayed)
    {
        var operation = delayed ? "schedule" : "send";
        var activityName = delayed ? ScheduleActivityName : SendActivityName;
        var parentContext = ResolveProducerParentContext(headers);

        return StartActivity(activityName, ActivityKind.Producer, parentContext, destination, operation, "send", messageId, headers);
    }

    public static Activity? StartProcess(BrokerEnvelope envelope, string receiveAddress)
    {
        var parentContext = ResolveRemoteParentContext(envelope.Headers);
        var activity = StartActivity(ProcessActivityName, ActivityKind.Consumer, parentContext, receiveAddress, "process", "process", envelope.MessageId, envelope.Headers);

        PropagateContextFromHeaders(activity, envelope.Headers);
        activity?.AddEvent(new ActivityEvent(HandoffEventName));

        return activity;
    }

    public static void AddProducerDispatchEvent(Activity? activity, DateTimeOffset? deliverAt)
    {
        if (activity is not { IsAllDataRequested: true })
        {
            return;
        }

        if (deliverAt.HasValue)
        {
            activity.AddEvent(new ActivityEvent(ScheduledEventName, tags: new ActivityTagsCollection
            {
                ["message.deliver_at"] = deliverAt.Value.ToString("O")
            }));
            return;
        }

        activity.AddEvent(new ActivityEvent(EnqueuedEventName));
    }

    public static void MarkError(Activity? activity, Exception ex, bool exceptionEscaped = true)
    {
        if (activity == null)
        {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, ex.Message);

        // Keep the cheap exception attributes always; the stacktrace (ex.ToString()) can be
        // large, so only materialize it when the span is fully recorded.
        var exceptionTags = new ActivityTagsCollection
        {
            ["exception.escaped"] = exceptionEscaped,
            ["exception.type"] = ex.GetType().FullName,
            ["exception.message"] = ex.Message,
        };

        if (activity.IsAllDataRequested)
        {
            exceptionTags["exception.stacktrace"] = ex.ToString();
        }

        activity.AddEvent(new ActivityEvent("exception", DateTimeOffset.UtcNow, exceptionTags));
        activity.SetTag(ErrorType, ex.GetType().FullName);
    }

    public static void MarkSuccess(Activity? activity)
    {
        if (activity == null)
        {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Ok);
    }

    // Context propagation (traceparent/tracestate/baggage) is hand-rolled to intentionally
    // mirror NServiceBus Core's internal ContextPropagation class byte-for-byte (same W3C
    // baggage serialization and span-based extraction). Do NOT switch to
    // DistributedContextPropagator here: Core itself does not use it, and diverging would
    // make this transport's injected/extracted baggage inconsistent with Core's pipeline.
    public static void PropagateContextToHeaders(Activity? activity, IDictionary<string, string> headers)
    {
        if (activity?.Id is not { } activityId)
        {
            return;
        }

        headers[Headers.DiagnosticsTraceParent] = activityId;

        if (activity.TraceStateString is not null)
        {
            headers[Headers.DiagnosticsTraceState] = activity.TraceStateString;
        }

        var baggage = string.Join(",", activity.Baggage.Select(item => $"{item.Key}={Uri.EscapeDataString(item.Value ?? string.Empty)}"));
        if (!string.IsNullOrEmpty(baggage))
        {
            headers[Headers.DiagnosticsBaggage] = baggage;
        }
    }

    static Activity? StartActivity(string activityName, ActivityKind kind, ActivityContext parentContext, string destination, string operationName, string operationType, string messageId, IReadOnlyDictionary<string, string> headers)
    {
        if (!activitySource.HasListeners())
        {
            return null;
        }

        var tags = new TagList
        {
            { MessagingSystem, NonDurableSystemName },
            { DestinationName, destination },
            { OperationName, operationName },
            { OperationType, operationType },
            { MessageId, messageId }
        };

        if (headers.TryGetValue(Headers.ConversationId, out var conversationId))
        {
            tags.Add(ConversationId, conversationId);
        }

        var activity = activitySource.CreateActivity(activityName, kind, parentContext, tags, links: null, idFormat: ActivityIdFormat.W3C);
        if (activity == null)
        {
            return null;
        }

        activity.DisplayName = operationName;
        activity.Start();
        return activity;
    }

    static ActivityContext ResolveProducerParentContext(IReadOnlyDictionary<string, string> headers)
    {
        if (Activity.Current is { } currentActivity)
        {
            return currentActivity.Context;
        }

        return ResolveRemoteParentContext(headers);
    }

    static ActivityContext ResolveRemoteParentContext(IReadOnlyDictionary<string, string> headers)
    {
        if (!headers.TryGetValue(Headers.DiagnosticsTraceParent, out var traceParent))
        {
            return default;
        }

        headers.TryGetValue(Headers.DiagnosticsTraceState, out var traceState);
        return ActivityContext.TryParse(traceParent, traceState, isRemote: true, out var parentContext) ? parentContext : default;
    }

    static void PropagateContextFromHeaders(Activity? activity, IReadOnlyDictionary<string, string> headers)
    {
        if (activity == null)
        {
            return;
        }

        if (!headers.TryGetValue(Headers.DiagnosticsBaggage, out var baggageValue))
        {
            return;
        }

        var baggageSpan = baggageValue.AsSpan();
        while (!baggageSpan.IsEmpty)
        {
            var lastComma = baggageSpan.LastIndexOf(',');
            ReadOnlySpan<char> baggageItem;

            if (lastComma >= 0)
            {
                baggageItem = baggageSpan[(lastComma + 1)..];
                baggageSpan = baggageSpan[..lastComma];
            }
            else
            {
                baggageItem = baggageSpan;
                baggageSpan = [];
            }

            var firstEquals = baggageItem.IndexOf('=');
            if (firstEquals < 0 || firstEquals >= baggageItem.Length)
            {
                continue;
            }

            var key = baggageItem[..firstEquals].Trim();
            var value = baggageItem[(firstEquals + 1)..];
            activity.AddBaggage(key.ToString(), Uri.UnescapeDataString(value));
        }
    }
}