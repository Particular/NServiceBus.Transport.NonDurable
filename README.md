# NServiceBus.Transport.NonDurable
A transport to exchange NServiceBus messages in memory, in a non-durable fashion. All messages are **lost** when the process ends, making this transport suitable for a very small range of scenarios in which all endpoints exist in a single process and the system can afford to lose data.
