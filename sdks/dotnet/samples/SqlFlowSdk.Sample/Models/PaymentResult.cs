// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace SqlFlowSdk.Sample.Models;

public class PaymentResult
{
    public required bool Success { get; set; }

    public string? TransactionId { get; set; }

    public string? ErrorMessage { get; set; }
}
