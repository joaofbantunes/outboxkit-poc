// --------------------------------------------------------------------------------------------------------------------
// <copyright company="Openvia">
//     Copyright (c) Openvia. All rights reserved.
// </copyright>
// --------------------------------------------------------------------------------------------------------------------

namespace YakShaveFx.OutboxKit.Core;

public sealed class OutboxSettings
{
    public int MaxBatchSize { get; init; } = 100;
    
    public TimeSpan PollingInterval { get; init; } = TimeSpan.FromMinutes(5);
}