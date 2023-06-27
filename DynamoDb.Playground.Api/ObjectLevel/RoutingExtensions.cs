using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace DynamoDb.Playground.Api.ObjectLevel;

public static class RoutingExtensions
{
    public static void MapWithObjectLevel(this IEndpointRouteBuilder routeBuilder)
    {
        routeBuilder.MapPost(
            "/objects/table",
        async (
                IAmazonDynamoDB client,
                CancellationToken cancellationToken
            ) =>
            {
                var request = new CreateTableRequest
                {
                    AttributeDefinitions = new List<AttributeDefinition>()
                    {
                        new AttributeDefinition
                        {
                            AttributeName = "PK",
                            AttributeType = ScalarAttributeType.S
                        },
                        new AttributeDefinition
                        {
                            AttributeName = "SK",
                            AttributeType = ScalarAttributeType.S
                        },
                        new AttributeDefinition
                        {
                            AttributeName = "OnSite",
                            AttributeType = ScalarAttributeType.N
                        },
                        new AttributeDefinition
                        {
                            AttributeName = "CreatedDateTimeOffset",
                            AttributeType = ScalarAttributeType.S
                        }
                    },
                    KeySchema = new List<KeySchemaElement>
                    {
                        new KeySchemaElement
                        {
                            AttributeName = "PK",
                            KeyType = KeyType.HASH //Partition key
                        },
                        new KeySchemaElement
                        {
                            AttributeName = "SK",
                            KeyType = KeyType.RANGE //Sort key
                        }
                    },
                    BillingMode = BillingMode.PAY_PER_REQUEST,
                    TableName = "table-records",
                    GlobalSecondaryIndexes = new List<GlobalSecondaryIndex>
                    {
                        new GlobalSecondaryIndex
                        {
                            IndexName = "onsite-gsi",
                            KeySchema = new List<KeySchemaElement>
                            {
                                new KeySchemaElement
                                {
                                    AttributeName = "OnSite",
                                    KeyType = KeyType.HASH 
                                }
                            },
                            Projection = new Projection
                            {
                                ProjectionType = ProjectionType.ALL
                            }
                        }
                    },
                    LocalSecondaryIndexes = new List<LocalSecondaryIndex>
                    {
                        new LocalSecondaryIndex
                        {
                            IndexName = "partitionkey-createdoffset-lsi",
                            KeySchema = new List<KeySchemaElement>
                            {
                                new KeySchemaElement
                                {
                                    AttributeName = "PK",
                                    KeyType = KeyType.HASH //Partition key
                                },
                                new KeySchemaElement
                                {
                                    AttributeName = "CreatedDateTimeOffset",
                                    KeyType = KeyType.RANGE //Sort key
                                }
                            },
                            Projection = new Projection
                            {
                                ProjectionType = ProjectionType.ALL
                            }
                        }
                    }
                };

                var response = await client.CreateTableAsync(request, cancellationToken);

                return Results.Ok(response);
            }
        )
            .WithTags("Object Level");

        routeBuilder.MapPost(
            "/objects",
            async (
                ObjectRecordRequest recordRequest,
                IDynamoDBContext context,
                CancellationToken cancellationToken
            ) =>
            {
                var record = new Record
                {
                    SortKey = Guid.NewGuid().ToString(),
                    Name = recordRequest.Name,
                    Location = recordRequest.Location,
                    OnSite = recordRequest.OnSite,
                    CreatedDateTime = DateTime.UtcNow,
                    CreatedDateTimeOffset = DateTimeOffset.UtcNow
                };

                await context.SaveAsync(record, cancellationToken);

                return Results.Created($"/objects/{record.SortKey}", recordRequest);
            }
        )
            .WithTags("Object Level");

        routeBuilder.MapPut(
            "/objects/{id}",
            async (
                string id,
                ObjectRecordRequest recordRequest,
                IDynamoDBContext context,
                CancellationToken cancellationToken
            ) =>
            {
                var record = new Record
                {
                    PartitionKey = "object",
                    SortKey = id,
                    Name = recordRequest.Name,
                    Location = recordRequest.Location,
                    OnSite = recordRequest.OnSite,
                    UpdatedDateTime = DateTime.UtcNow
                };

                await context.SaveAsync(record, cancellationToken);

                return Results.Ok(
                    new ObjectRecordResponse
                    {
                        Id = id,
                        Name = recordRequest.Name,
                        Location = recordRequest.Location,
                        UpdatedDateTime = record.UpdatedDateTime
                    }
                );
            }
        )
            .WithTags("Object Level");

        routeBuilder.MapGet(
            "/objects/{id}",
            async (string id, IDynamoDBContext context, CancellationToken cancellationToken) =>
            {
                var response = await context.LoadAsync(new Record { PartitionKey = "object", SortKey = id }, cancellationToken);
                if (response == null)
                {
                    return Results.NotFound();
                }

                return Results.Ok(
                    new ObjectRecordResponse
                    {
                        Id = id,
                        Name = response.Name,
                        Location = response.Location!,
                        OnSite = response.OnSite,
                        CreatedDateTime = response.CreatedDateTime,
                        CreatedDateTimeOffset = response.CreatedDateTimeOffset,
                        UpdatedDateTime = response.UpdatedDateTime
                    }
                );
            }
        )
            .WithTags("Object Level");
    }
}

public class ObjectRecordRequest
{
    public required string Name { get; set; }
    public required string Location { get; set; }
    public bool OnSite { get; set; }
}

public class ObjectRecordResponse : ObjectRecordRequest
{
    public string? Id { get; set; }
    public DateTime CreatedDateTime { get; set; }
    public DateTimeOffset CreatedDateTimeOffset { get; set; }
    public DateTime? UpdatedDateTime { get; set; }
}

[DynamoDBTable("table-records")]
public class Record
{
    [DynamoDBHashKey("PK")]
    public string PartitionKey { get; set; } = "object";
    [DynamoDBRangeKey("SK")]
    public string SortKey { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string Location { get; set; } = default!;
    [DynamoDBGlobalSecondaryIndexHashKey("onsite-gsi")]
    public bool OnSite { get; set; }
    public DateTime CreatedDateTime { get; set; }
    [DynamoDBLocalSecondaryIndexRangeKey("partitionkey-createdoffset-lsi")]
    [DynamoDBProperty(typeof(DateTimeOffsetConverter))]
    public DateTimeOffset CreatedDateTimeOffset { get; set; }
    public DateTime? UpdatedDateTime { get; set; }
}

public class DateTimeOffsetConverter : IPropertyConverter
{
    public object FromEntry(DynamoDBEntry entry)
    {
        var dateTime = entry?.AsString();
        if (string.IsNullOrEmpty(dateTime))
            return null;

        if (!DateTimeOffset.TryParse(dateTime, out DateTimeOffset value))
            throw new ArgumentException("entry parameter must be a validate DateTimeOffset value.", nameof(entry));

        return value;
    }

    public DynamoDBEntry ToEntry(object value)
    {
        if (value == null)
            return new DynamoDBNull();

        if (value.GetType() != typeof(DateTimeOffset) && value.GetType() != typeof(DateTimeOffset?))
            throw new ArgumentException("value parameter must be a DateTimeOffset or a Nullable<DateTimeOffset>.", nameof(value));

        return ((DateTimeOffset)value).ToString("O");
    }
}