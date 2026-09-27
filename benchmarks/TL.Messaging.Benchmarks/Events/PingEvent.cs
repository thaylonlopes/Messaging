using System;

namespace TL.Messaging.Benchmarks.Events;

/// <summary>
/// Evento ultraleve para medição de latência mínima e overhead de empacotamento em nanossegundos.
/// </summary>
public record PingEvent(Guid PingId, long TimestampTicks);
