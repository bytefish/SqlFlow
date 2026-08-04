// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Sample.Models;

public class OrderData
{
    public required string OrderId { get; set; }

    public string CustomerEmail { get; set; } = string.Empty;

    public bool IsPremium { get; set; } = false;

    public decimal Amount { get; set; }

    public List<string> Items { get; set; } = new();
}
