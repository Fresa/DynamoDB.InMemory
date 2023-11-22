using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Nodes;
using Json.Pointer;

namespace DynamoDB.InMemory;

internal static class JsonNodeExtensions
{
    internal static bool TryEvaluate(this JsonNode node, PointerSegment segment,
        [NotNullWhen(true)] out JsonNode? resolvedNode) =>
        node.TryEvaluate(new[] { segment }, out resolvedNode);

    internal static bool TryEvaluate(this JsonNode node, PointerSegment[] segments,
        [NotNullWhen(true)] out JsonNode? resolvedNode) =>
        node.TryEvaluate(JsonPointer.Create(segments), out resolvedNode);

    internal static bool TryEvaluate(this JsonNode node, JsonPointer pointer,
        [NotNullWhen(true)] out JsonNode? resolvedNode)
    {
        if (pointer
                .TryEvaluate(node, out resolvedNode) && resolvedNode != null)
        {
            return true;
        }

        resolvedNode = null;
        return false;
    }

    internal static JsonNode Evaluate(this JsonNode node, JsonPointer pointer)
    {
        if (node.TryEvaluate(pointer, out var resolvedNode))
        {
            return resolvedNode;
        }

        throw new InvalidOperationException(
            $"{pointer} does not exist in json {node} or is null");
    }
    internal static JsonNode Evaluate(this JsonNode node, params PointerSegment[] segments)
    {
        if (node.TryEvaluate(segments, out var resolvedNode))
        {
            return resolvedNode;
        }

        throw new InvalidOperationException(
            $"{JsonPointer.Create(segments)} does not exist in json {node} or is null");
    }

    internal static JsonNode Evaluate(this JsonNode node, PointerSegment segment) =>
        node.Evaluate(new[] { segment });

    internal static IEnumerable<JsonNode> GetChildrenValues(this JsonNode node) =>
        GetChildren(node).Select(childNode => childNode.Value);

    internal static IEnumerable<(string Key, JsonNode Value)> GetChildren(this JsonNode node)
    {
        switch (node)
        {
            case JsonArray array:
                for (var i = 0; i < array.Count; i++)
                {
                    var item = array[i];
                    if (item != null)
                    {
                        yield return (i.ToString(), item);
                    }
                }

                break;
            case JsonObject @object:
                foreach (var child in @object)
                {
                    if (child.Value != null)
                    {
                        yield return (child.Key, child.Value);
                    }
                }

                break;
            case JsonValue:
                throw new InvalidOperationException("Current node is a value and hence has no children");
            default:
                throw new NotImplementedException($"Nodes of type {node.GetType()} is currently not supported");
        }
    }
}