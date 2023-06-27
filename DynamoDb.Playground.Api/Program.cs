using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using DynamoDb.Playground.Api.ObjectLevel;

var builder = WebApplication.CreateBuilder(args);

var awsOptions = builder.Configuration.GetAWSOptions();
builder.Services.AddDefaultAWSOptions(awsOptions);

var dynamoDbConfig = builder.Configuration.GetSection("DynamoDb").Get<DynamoDbSettings>() ?? new DynamoDbSettings();

if (dynamoDbConfig.LocalMode)
{
    var clientConfig = new AmazonDynamoDBConfig
    {
        ServiceURL = dynamoDbConfig.ServiceUrl
    };
    builder.Services.AddSingleton<IAmazonDynamoDB>(_ => new AmazonDynamoDBClient("123", "123", clientConfig));
}
else
{
    builder.Services.AddAWSService<IAmazonDynamoDB>();
}

builder.Services.AddScoped<IDynamoDBContext>(sp => new DynamoDBContext(sp.GetRequiredService<IAmazonDynamoDB>(), new DynamoDBContextConfig { IgnoreNullValues = true }));

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}


app.MapWithObjectLevel();

app.Run();

