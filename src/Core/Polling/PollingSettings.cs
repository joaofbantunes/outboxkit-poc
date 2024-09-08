// --------------------------------------------------------------------------------------------------------------------
// <copyright company="Openvia">
//     Copyright (c) Openvia. All rights reserved.
// </copyright>
// --------------------------------------------------------------------------------------------------------------------

namespace YakShaveFx.OutboxKit.Core.Polling;

public sealed class PollingSettings
{
    public TimeSpan PollingInterval { get; init; } = TimeSpan.FromMinutes(5);
}