namespace NServiceBus.Transport.NonDurable.Tests.API;

using NUnit.Framework;
using Particular.Approvals;
using PublicApiGenerator;

[TestFixture]
public class APIApprovals
{
    [Test]
    public void ApproveNServiceBus()
    {
        var publicApi = typeof(NonDurableBroker).Assembly.GeneratePublicApi(new ApiGeneratorOptions
        {
            ExcludeAttributes = ["System.Runtime.Versioning.TargetFrameworkAttribute", "System.Reflection.AssemblyMetadataAttribute"]
        });

        Approver.Verify(publicApi);
    }
}