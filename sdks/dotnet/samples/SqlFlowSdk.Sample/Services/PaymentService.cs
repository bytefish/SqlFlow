// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.Sample.Models;

namespace SqlFlowSdk.Sample.Services;

public class PaymentService
{
    public async Task<PaymentResult> ChargeAsync(string orderId, decimal amount)
    {
        await Task.Delay(500);

        return new PaymentResult
        {
            Success = true,
            TransactionId = $"txn_{Guid.NewGuid()}"
        };
    }
}