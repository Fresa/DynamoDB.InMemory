using System.Collections.Concurrent;
using System.Text;
using System.Text.Json.Nodes;

namespace DynamoDB.InMemory;

public sealed class DynamoDb
{
    private readonly ConcurrentDictionary<string, DynamoDbTable> _tables = new();
    public string Region { get; }
    public string Host { get; }
    public DynamoDb(string region)
    {
        Region = region;
        Host = $"dynamodb.{region.ToLower()}.amazonaws.com";
    }

    internal void CreateTable(JsonNode createRequest)
    {
        var table = DynamoDbTable.Create(Region, createRequest);
        if (!_tables.TryAdd(table.TableName, table))
            throw new InvalidOperationException($"Table {table.TableName} has already been created");
    }

    public async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (request.RequestUri == null)
            return DynamoDbTable.CreateInternalFailureResponse("Request URI missing");
        if (request.Content == null)
            return DynamoDbTable.CreateInternalFailureResponse("Content missing");
        var host = request.RequestUri.Host;
        if (host != Host)
            return DynamoDbTable.CreateValidationErrorResponse($"Got request host {host} towards a DynamoDB instance in {Host}");
        var requestValue =
            JsonNode.Parse(await request.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false));
        if (requestValue == null)
            return DynamoDbTable.CreateValidationErrorResponse("Could not parse content as json");
        if (!requestValue.TryEvaluate("TableName", out var tableNameNode))
            return DynamoDbTable.CreateValidationErrorResponse("Request is missing TableName");
        var tableName = tableNameNode.GetValue<string>();
        DynamoDbTable table;
        try
        {
            table = _tables.GetOrAdd(tableName, _ => DynamoDbTable.Create(Region, requestValue));
        }
        // Failed create validation
        catch (ValidationErrorException e)
        {
            return DynamoDbTable.CreateValidationErrorResponse(e.Message);
        }
        return await table.SendAsync(request, requestValue, cancellationToken)
            .ConfigureAwait(false);
    }

    internal Task<HttpResponseMessage> GetAsync(JsonNode request, CancellationToken cancellationToken) =>
        SendAsync(request, DynamoDbTable.GetKey, cancellationToken);
    internal Task<HttpResponseMessage> GetAsync(string jsonFormattedGetRequest, CancellationToken cancellationToken) =>
        SendAsync(jsonFormattedGetRequest, DynamoDbTable.GetKey, cancellationToken);

    internal Task<HttpResponseMessage> QueryAsync(JsonNode request, CancellationToken cancellationToken) =>
        SendAsync(request, DynamoDbTable.QueryKey, cancellationToken);
    internal Task<HttpResponseMessage> QueryAsync(string jsonFormattedQueryRequest, CancellationToken cancellationToken) =>
        SendAsync(jsonFormattedQueryRequest, DynamoDbTable.QueryKey, cancellationToken);

    internal Task<HttpResponseMessage> UpdateAsync(JsonNode request, CancellationToken cancellationToken) =>
        SendAsync(request, DynamoDbTable.UpdateKey, cancellationToken);
    internal Task<HttpResponseMessage> UpdateAsync(string jsonFormattedUpdate, CancellationToken cancellationToken) =>
        SendAsync(jsonFormattedUpdate, DynamoDbTable.UpdateKey, cancellationToken);

    private Task<HttpResponseMessage> SendAsync(JsonNode request, string target, CancellationToken cancellationToken) =>
        SendAsync(request.ToJsonString(), target, cancellationToken);

    private Task<HttpResponseMessage> SendAsync(string jsonFormattedRequest, string target, CancellationToken cancellationToken)
    {
        return SendAsync(new HttpRequestMessage(HttpMethod.Post, new Uri($"https://{Host}"))
        {
            Headers =
            {
                { "X-Amz-Target", target }
            },
            Content = new StringContent(jsonFormattedRequest, Encoding.UTF8)
        }, cancellationToken);
    }
}

internal sealed class ValidationErrorException : Exception
{
    public ValidationErrorException(string validationError) : base(validationError)
    {
    }
}