using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using System.Windows.Markup;
using Json.More;
using Json.Pointer;

namespace DynamoDB.InMemory;

internal sealed class DynamoDbTable
{
    private const string Version = "20120810";
    private const string TargetPrefix = $"DynamoDB_{Version}";
    internal const string UpdateKey = $"{TargetPrefix}.UpdateItem";
    internal const string GetKey = $"{TargetPrefix}.GetItem";
    internal const string DeleteKey = $"{TargetPrefix}.DeleteItem";
    internal const string QueryKey = $"{TargetPrefix}.Query";
    internal const string DescribeTableKey = $"{TargetPrefix}.DescribeTable";

    private readonly ConcurrentDictionary<string, OnReceiveAsyncHandler> _actions = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, JsonNode>> _partitions = new();

    private DynamoDbTable(
        KeySchema primaryKey,
        JsonNode attributeDefinitions,
        JsonNode keySchema)
    {
        _primaryKey = primaryKey;
        _attributeDefinitions = attributeDefinitions;
        _keySchema = keySchema;
        _actions.TryAdd(UpdateKey, UpdateAsync);
        _actions.TryAdd(GetKey, GetAsync);
        _actions.TryAdd(QueryKey, QueryAsync);
        _actions.TryAdd(DeleteKey, DeleteAsync);
        _actions.TryAdd(DescribeTableKey, DescribeTableAsync);
    }

    internal required string TableName { get; init; }
    internal required string Region { get; init; }
    private readonly KeySchema _primaryKey;
    private readonly JsonNode _attributeDefinitions;
    private readonly JsonNode _keySchema;
    private readonly long _creationDateTime = DateTimeOffset.UnixEpoch.ToUnixTimeSeconds();
    private IDictionary<string, KeySchema> GlobalSecondaryIndices { get; init; } =
        ImmutableDictionary<string, KeySchema>.Empty;

    private (string PartitionKey, string RangeKey) GetPrimaryKey(JsonNode item)
    {
        var primaryKey = item.Evaluate(_primaryKey.PartitionKey).GetValue<string>();
        if (_primaryKey.RangeKey != JsonPointer.Empty)
        {
            return (primaryKey, item.Evaluate(_primaryKey.RangeKey).GetValue<string>());
        }
        return (primaryKey, string.Empty);
    }

    private delegate Task<HttpResponseMessage> OnReceiveAsyncHandler(JsonNode request, CancellationToken
        cancellation);

    internal async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, JsonNode content, CancellationToken cancellationToken)
    {
        if (!request.Headers.TryGetValues("X-Amz-Target", out var target))
            return CreateValidationErrorResponse("Request is missing header X-Amz-Target");

        var action = target.FirstOrDefault();
        if (action == null)
            return CreateValidationErrorResponse("Request header X-Amz-Target has no value");

        if (!_actions.TryGetValue(action, out var handler))
            return CreateValidationErrorResponse($"Received request with X-Amz-Target {action} which is not supported");

        return await handler.Invoke(content, cancellationToken)
            .ConfigureAwait(false);
    }

    internal static DynamoDbTable Create(string region, JsonNode createRequest)
    {
        var tableName = createRequest.Evaluate("TableName").GetValue<string>();
        var keySchemaNode = createRequest.Evaluate("KeySchema");
        var attributeDefinitionsNode = createRequest.Evaluate("AttributeDefinitions");

        var keySchema = ParseKeySchema(keySchemaNode, attributeDefinitionsNode);

        var globalSecondaryIndices = ImmutableDictionary<string, KeySchema>.Empty;
        if (createRequest.TryEvaluate("GlobalSecondaryIndexes", out var globalSecondaryIndexesNode))
        {
            globalSecondaryIndices = globalSecondaryIndexesNode
                .GetChildrenValues()
                .ToImmutableDictionary(
                    node =>
                        node.Evaluate("IndexName").GetValue<string>(),
                    node =>
                    {
                        var gsiKeySchemaNode = node.Evaluate("KeySchema");
                        return ParseKeySchema(gsiKeySchemaNode, attributeDefinitionsNode);
                    });
        }

        var table = new DynamoDbTable(
            keySchema,
            attributeDefinitions: attributeDefinitionsNode,
            keySchema: keySchemaNode)
        {
            TableName = tableName,
            Region = region,
            GlobalSecondaryIndices = globalSecondaryIndices
        };
        return table;
    }

    private static KeySchema ParseKeySchema(JsonNode keySchemaNode, JsonNode attributeDefinitionsNode)
    {
        var keySchema = keySchemaNode.GetChildrenValues().ToList();
        var (partitionKeySchema, sortKeySchema) = keySchema.Count switch
        {
            0 => throw new ArgumentException("KeySchema must contain a HASH key"),
            1 => (keySchema[0], null),
            2 => (keySchema[0], keySchema[1]),
            _ => throw new ArgumentException($"KeySchema can not contain more than one HASH and one RANGE key, got {keySchema.Count} keys")
        };

        var attributeDefinitions = attributeDefinitionsNode.GetChildrenValues().ToList();

        if (partitionKeySchema.Evaluate("KeyType").GetValue<string>() != "HASH")
        {
            throw new ArgumentException("First key must be a HASH");
        }
        var partitionKeyName = partitionKeySchema.Evaluate("AttributeName").GetValue<string>();
        var partitionKeyDefinition =
            attributeDefinitions.First(node => node.Evaluate("AttributeName").GetValue<string>() == partitionKeyName);
        var partitionKeyAttributeType = partitionKeyDefinition.Evaluate("AttributeType").GetValue<string>();
        if (partitionKeyAttributeType != "S" &&
             partitionKeyAttributeType != "N" &&
             partitionKeyAttributeType != "B")
        {
            throw new ArgumentException("AttributeType must be S or N or B");
        }

        var partitionKeyPointer = JsonPointer.Create(partitionKeyDefinition.Evaluate("AttributeName").GetValue<string>(),
            partitionKeyAttributeType);
        var partitionKeyAttributeTypePointer = JsonPointer.Create(partitionKeyAttributeType);

        var sortKeyPointer = JsonPointer.Empty;
        var sortKeyAttributeTypePointer = JsonPointer.Empty;

        if (sortKeySchema != null)
        {
            if (sortKeySchema.Evaluate("KeyType").GetValue<string>() != "RANGE")
            {
                throw new ArgumentException("Second key type must be RANGE");
            }

            var sortKeyName = sortKeySchema.Evaluate("AttributeName").GetValue<string>();
            var sortKeyDefinition =
                attributeDefinitions.First(node => node.Evaluate("AttributeName").GetValue<string>() == sortKeyName);
            var sortKeyAttributeType = sortKeyDefinition.Evaluate("AttributeType").GetValue<string>();
            sortKeyPointer = JsonPointer.Create(sortKeyDefinition.Evaluate("AttributeName").GetValue<string>(),
                sortKeyDefinition.Evaluate("AttributeType").GetValue<string>());
            sortKeyAttributeTypePointer = JsonPointer.Create(sortKeyAttributeType);
        }
        return new KeySchema
        {
            PartitionKey = partitionKeyPointer,
            PartitionKeyAttributeValue = partitionKeyAttributeTypePointer,
            RangeKey = sortKeyPointer,
            RangeKeyAttributeValue = sortKeyAttributeTypePointer,
        };
    }

    private Task<HttpResponseMessage> UpdateAsync(JsonNode requestValue, CancellationToken
        cancellation)
    {
        var keyNode = requestValue.Evaluate("Key");
        var (partitionKey, rangeKey) = GetPrimaryKey(keyNode);
        var conditionalExpectations = new List<Func<JsonNode, bool>>();
        if (requestValue.TryEvaluate("Expected", out var expectedNode))
        {
            foreach (var (expectationPropertyName, expectationExpression) in expectedNode.GetChildren())
            {
                if (expectationExpression.TryEvaluate("Exists", out var existsCondition))
                {
                    var shouldExist = existsCondition.GetValue<bool>();
                    conditionalExpectations.Add(node => node.TryEvaluate(expectationPropertyName, out _) == shouldExist);
                }
            }
        }

        if (!requestValue.TryEvaluate("ExpressionAttributeValues", out var expressionAttributeValues))
        {
            expressionAttributeValues = new JsonObject();
        }
        if (requestValue.TryEvaluate("ConditionExpression", out var conditionExpression))
        {
            conditionalExpectations.Add(item => MeetsCondition(item, conditionExpression.GetValue<string>(), expressionAttributeValues));
        }

        try
        {
            _partitions.AddOrUpdate(partitionKey, _ =>
            {
                var partition = new ConcurrentDictionary<string, JsonNode>();
                JsonNode item = new JsonObject();
                CheckConditions(item);
                item = Update(item);
                partition.TryAdd(rangeKey, item);
                return partition;
            }, (_, partition) =>
            {
                partition.AddOrUpdate(rangeKey, s =>
                {
                    JsonNode item = new JsonObject();
                    CheckConditions(item);
                    return Update(item);
                }, (s, node) =>
                {
                    CheckConditions(node);
                    // We snapshot the existing item as other functions might iterate on the item's property
                    // collections which are not thread safe and hence will throw if mutated concurrently
                    return Update(node.Copy()!);
                });
                return partition;
            });
        }
        catch (ConditionalCheckFailedException)
        {
            return Task.FromResult(CreateConditionalCheckFailedResponse());
        }
        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));

        void CheckConditions(JsonNode item)
        {
            if (!conditionalExpectations.All(expected => expected(item)))
                throw new ConditionalCheckFailedException();
        }

        JsonNode Update(JsonNode item)
        {
            if (requestValue.TryEvaluate("AttributeUpdates", out var attributesUpdates))
            {
                foreach (var (attributeName, attributeValue) in attributesUpdates.GetChildren())
                {
                    var value = attributeValue.Evaluate("Value").Copy()!;
                    var action = attributeValue.Evaluate("Action").GetValue<string>();
                    switch (action)
                    {
                        case "PUT":
                            item[attributeName] = value;
                            break;
                    }
                }
                return item;
            }

            if (requestValue.TryEvaluate("UpdateExpression", out var updateExpressionNode))
            {
                var updateExpressions = new Queue<string>(new[]
                {
                    updateExpressionNode.GetValue<string>()
                });
                foreach (var functionName in new[] { "SET", "ADD", "REMOVE", "DELETE" })
                {
                    var count = updateExpressions.Count;
                    for (var i = 0; i < count; i++)
                    {
                        var updateExpression = updateExpressions.Dequeue();
                        if (updateExpression.Contains(functionName, StringComparison.InvariantCulture))
                        {
                            var functionExpressions =
                                updateExpression.Split($"{functionName} ", StringSplitOptions.RemoveEmptyEntries);
                            foreach (var functionExpression in functionExpressions)
                            {
                                updateExpressions.Enqueue($"{functionName} {functionExpression}");
                            }
                        }
                        else
                        {
                            updateExpressions.Enqueue(updateExpression);
                        }
                    }
                }

                while (updateExpressions.TryDequeue(out var updateExpression))
                {
                    var function = updateExpression[..updateExpression.IndexOf(' ')];
                    var functionExpressions = updateExpression[function.Length..].Split(',');
                    foreach (var functionExpression in functionExpressions)
                    {
                        var tokens = functionExpression.Split(' ', StringSplitOptions.RemoveEmptyEntries);

                        switch (function)
                        {
                            case "SET":
                                var attributeToSet = tokens[0];
                                var operation = tokens[1];
                                var attributeReference = tokens[2];

                                if (operation != "=")
                                    throw new NotSupportedException($"Operation {operation} is not supported");
                                var value = expressionAttributeValues.Evaluate(attributeReference);
                                item[attributeToSet] = value.Copy();
                                break;
                            case "REMOVE":
                                var attributeToRemove = tokens[0];
                                item.AsObject().Remove(attributeToRemove);
                                break;
                            default:
                                throw new NotSupportedException($"Function {function} is not supported");
                        }
                    }
                }

                return item;
            }

            throw new InvalidOperationException("Missing AttributeUpdates and UpdateExpression");
        }
    }

    private static bool MeetsCondition(JsonNode item, string conditionExpression, JsonNode expressionAttributeValues)
    {
        // only supports the most simple conditions
        var conditions = conditionExpression.Split("AND");
        foreach (var condition in conditions)
        {
            var tokens = condition.Split(' ');
            var key = tokens[0];
            var @operator = tokens[1];
            var valueToken = tokens[2];
            // we don't bother with types just assume a simple node and get it's first value
            var value = expressionAttributeValues.Evaluate(valueToken).GetChildrenValues().First().GetValue<string>();
            var conditionMet = @operator switch
            {
                "=" => Compare() == 0,
                "<" => Compare() < 0,
                "<=" => Compare() <= 0,
                ">" => Compare() > 0,
                ">=" => Compare() >= 0,
                _ => throw new InvalidOperationException($"Comparer {@operator} is not a valid token")
            };
            if (!conditionMet)
                return false;

            int Compare() => string.Compare(GetValue(), value, StringComparison.Ordinal);
            string GetValue() => item.Evaluate(key).GetChildrenValues().First().GetValue<string>();
        }

        return true;
    }

    private Task<HttpResponseMessage> GetAsync(JsonNode requestValue, CancellationToken
        cancellation)
    {
        var keyNode = requestValue.Evaluate("Key");
        var (partitionKey, rangeKey) = GetPrimaryKey(keyNode);

        JsonNode? item = null;
        if (_partitions.TryGetValue(partitionKey, out var partition))
        {
            partition.TryGetValue(rangeKey, out item);
        }

        var response = $$"""
            {
                "ConsumedCapacity": {
                    "CapacityUnits": 1,
                    "TableName": "{{TableName}}"
                },
                "Item": {{item?.ToJsonString() ?? "null"}}
            }
            """;
        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(response, Encoding.UTF8)
        });
    }

    private Task<HttpResponseMessage> QueryAsync(JsonNode requestValue, CancellationToken
        cancellation)
    {
        var keys = _primaryKey;
        IEnumerable<JsonNode> items = Enumerable.Empty<JsonNode>();
        if (requestValue.TryEvaluate("IndexName", out var indexNode))
        {
            var indexName = indexNode.GetValue<string>();
            if (!GlobalSecondaryIndices.TryGetValue(indexName, out var gsiKeys))
            {
                return Task.FromResult(CreateResourceNotFoundResponse(
                    $"Global secondary index {indexName} does not exist for table {TableName}"));
            }

            keys = gsiKeys;
        }

        if (requestValue.TryEvaluate("KeyConditionExpression", out var keyConditionExpressionNode))
        {
            var expressionAttributeValues = requestValue.Evaluate("ExpressionAttributeValues");

            var keyConditionExpression = keyConditionExpressionNode.GetValue<string>();
            var keyConditionExpressionSplit = keyConditionExpression.IndexOf("AND", StringComparison.InvariantCulture);
            var length = keyConditionExpressionSplit == -1 ? keyConditionExpression.Length : keyConditionExpressionSplit;
            var partitionKeyExpression = keyConditionExpression[..length];
            var partitionKeyExpressionTokens = partitionKeyExpression.Split(' ');
            var partitionKey = partitionKeyExpressionTokens[0];
            var partitionKeyAttributeKey = partitionKeyExpressionTokens[2];
            var partitionKeyAttribute = expressionAttributeValues.Evaluate(partitionKeyAttributeKey);

            var partitionKeyValue = partitionKeyAttribute.Evaluate(keys.PartitionKeyAttributeValue).GetValue<string>();
            items = FilterAndSortItems(partitionKeyValue);

            if (keyConditionExpressionSplit > -1)
            {
                if (keys.RangeKey == JsonPointer.Empty)
                {
                    return Task.FromResult(CreateValidationErrorResponse("Request defines a range key condition but the table/index has no range key"));
                }
                var rangeKeyExpression = keyConditionExpression[keyConditionExpressionSplit..];
                var rangeKeyExpressionTokens = rangeKeyExpression.Split(' ')[1..];
                var rangeKey = rangeKeyExpressionTokens[0];
                var comparisonToken = rangeKeyExpressionTokens[1];
                if (rangeKey == "begins_with")
                {
                    var rangeKeyAttributeKey = rangeKeyExpressionTokens[3];
                    var rangeKeyAttribute = expressionAttributeValues.Evaluate(rangeKeyAttributeKey);
                    var sortKeyValue = rangeKeyAttribute.Evaluate(keys.RangeKeyAttributeValue).GetValue<string>();
                    items = items.SkipWhile(item => !item.Evaluate(keys.RangeKey).GetValue<string>().StartsWith(sortKeyValue));
                }
                else if (comparisonToken == "BETWEEN")
                {
                    var firstRangeKeyAttributeKey = rangeKeyExpressionTokens[2];
                    var secondRangeKeyAttributeKey = rangeKeyExpressionTokens[4];
                    var firstRangeKeyAttribute = expressionAttributeValues.Evaluate(firstRangeKeyAttributeKey);
                    var secondRangeKeyAttribute = expressionAttributeValues.Evaluate(secondRangeKeyAttributeKey);
                    var firstSortKeyValue = firstRangeKeyAttribute.Evaluate(keys.RangeKeyAttributeValue).GetValue<string>();
                    var secondSortKeyValue = secondRangeKeyAttribute.Evaluate(keys.RangeKeyAttributeValue).GetValue<string>();
                    items = items
                        .SkipWhile(item =>
                            string.Compare(item.Evaluate(keys.RangeKey).GetValue<string>(), firstSortKeyValue, StringComparison.Ordinal) >= 0)
                        .TakeWhile(pair =>
                            string.Compare(pair.Evaluate(keys.RangeKey).GetValue<string>(), secondSortKeyValue, StringComparison.Ordinal) < 0);
                }
                else
                {
                    var rangeKeyAttributeKey = rangeKeyExpressionTokens[2];
                    var rangeKeyAttribute = expressionAttributeValues.Evaluate(rangeKeyAttributeKey);
                    var sortKeyValue = rangeKeyAttribute.Evaluate(keys.RangeKeyAttributeValue).GetValue<string>();
                    items = comparisonToken switch
                    {
                        "=" => items.SkipWhile(item => CompareRangeKey(item) != 0)
                            .TakeWhile(item => CompareRangeKey(item) == 0),
                        "<" => items.TakeWhile(item => CompareRangeKey(item) < 0),
                        "<=" => items.TakeWhile(item => CompareRangeKey(item) <= 0),
                        ">" => items.SkipWhile(item => CompareRangeKey(item) <= 0),
                        ">=" => items.SkipWhile(item => CompareRangeKey(item) < 0),
                        _ => throw new InvalidOperationException($"Comparer {comparisonToken} is not a valid token")
                    };

                    int CompareRangeKey(JsonNode item) => string.Compare(GetRangeKeyValue(item), sortKeyValue, StringComparison.Ordinal);
                    string GetRangeKeyValue(JsonNode item) => item.Evaluate(keys.RangeKey).GetValue<string>();
                }
            }
        }

        if (requestValue.TryEvaluate("ExclusiveStartKey", out var exclusiveStartKey))
        {
            var (partitionKey, sortKey) = GetPrimaryKey(exclusiveStartKey);
            items = FilterAndSortItems(partitionKey)
                .SkipWhile(item =>
                    item.Evaluate(keys.RangeKey).GetValue<string>() != sortKey);
        }

        var responseItems = items.ToArray();
        var response = $$"""
            {
                "Items": [
                    {{string.Join(',', responseItems.Select(item => item.ToJsonString()))}}
                ],
                "Count": {{responseItems.Length}}
            }
            """;
        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(response, Encoding.UTF8)
        });

        IEnumerable<JsonNode> FilterAndSortItems(string partitionKeyValue)
        {
            var partition = _partitions.GetOrAdd(partitionKeyValue, new ConcurrentDictionary<string, JsonNode>());
            return partition.Values
                .Where(value =>
                    value.TryEvaluate(keys.PartitionKey, out var itemPartitionKey) &&
                    itemPartitionKey.GetValue<string>() == partitionKeyValue)
                .OrderBy(pair => keys.RangeKey == JsonPointer.Empty ?
                    string.Empty :
                    pair.Evaluate(keys.RangeKey).GetValue<string>());
        }
    }


    private Task<HttpResponseMessage> DeleteAsync(JsonNode requestValue, CancellationToken
        cancellation)
    {
        var (partitionKey, rangeKey) = GetPrimaryKey(requestValue);
        if (_partitions.TryGetValue(partitionKey, out var partition))
        {
            partition.TryRemove(rangeKey, out _);
        }

        var response = $$"""
            {}
            """;
        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
        {

            Content = new StringContent(response, Encoding.UTF8)
        });
    }

    private Task<HttpResponseMessage> DescribeTableAsync(JsonNode request, CancellationToken
        cancellation)
    {
        var response = $$"""
            {
                "Table" : {
                    "TableArn": "arn:aws:dynamodb:{{Region}}:123456789012:table/{{TableName}}",
                    "AttributeDefinitions": {{_attributeDefinitions.ToJsonString()}},
                    "CreationDateTime": {{_creationDateTime}},
                    "ItemCount": {{_partitions.Count}},
                    "KeySchema": {{_keySchema.ToJsonString()}},
                    "ProvisionedThroughput": {
                        "NumberOfDecreasesToday": 0,
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5
                    },
                    "TableName": "{{TableName}}",
                    "TableSizeBytes": 0,
                    "TableStatus": "ACTIVE"
                }
            }
            """;
        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(response, Encoding.UTF8)
        });
    }

    private record KeySchema
    {
        public required JsonPointer PartitionKey { get; init; }
        public required JsonPointer PartitionKeyAttributeValue { get; init; }
        public JsonPointer RangeKey { get; init; } = JsonPointer.Empty;
        public JsonPointer RangeKeyAttributeValue { get; init; } = JsonPointer.Empty;
    }

    private static class ErrorComponents
    {
        private const string Namespace = "com.amazonaws.dynamodb";
        private const string Prefix = $"{Namespace}.v{Version}";
        internal static class Codes
        {
            internal const string ConditionalCheckFailed = $"{Prefix}#ConditionalCheckFailedException";
            internal const string ResourceNotFound = $"{Prefix}#ResourceNotFoundException";
            internal const string ValidationError = $"{Prefix}#ValidationError";
            internal const string InternalFailure = $"{Prefix}#InternalFailure";
        }
    }

    private static HttpResponseMessage CreateBadRequestResponse(string type, string message) =>
        CreateErrorResponse(HttpStatusCode.BadGateway, type, message);
    private static HttpResponseMessage CreateErrorResponse(HttpStatusCode statusCode, string type, string message) =>
        new(statusCode)
        {
            Content = new StringContent($$"""
                    {
                        "__type": "{{type}}",
                        "message": "{{message}}"
                    }
                    """)
        };

    private static HttpResponseMessage CreateConditionalCheckFailedResponse() =>
        CreateBadRequestResponse(ErrorComponents.Codes.ConditionalCheckFailed, "Version did not match");
    private static HttpResponseMessage CreateResourceNotFoundResponse(string message) =>
        CreateBadRequestResponse(ErrorComponents.Codes.ResourceNotFound, message);
    internal static HttpResponseMessage CreateValidationErrorResponse(string message) =>
        CreateBadRequestResponse(ErrorComponents.Codes.ValidationError, message);
    internal static HttpResponseMessage CreateInternalFailureResponse(string message) =>
        CreateErrorResponse(HttpStatusCode.InternalServerError, ErrorComponents.Codes.InternalFailure, message);
}