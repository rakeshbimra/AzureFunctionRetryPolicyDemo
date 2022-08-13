# AzureFunctionRetryPolicyDemo

In this demo we have created Polly retry policy for Azure Service Bus Topic Trigger. If there is 429 CosmosDb exception during processing the request, these policies will retry those requests.
There are two policies
1. GetExponentialBackoffRetryPolicy
2. GetThrottlingAwareRetryPolicy
