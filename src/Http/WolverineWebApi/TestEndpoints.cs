using Marten;
using Wolverine.Http;

namespace WolverineWebApi;

[SpecialModifyHttpChain]
public static class TestEndpoints
{
    [WolverineGet("/hello")]
    public static string Speak()
    {
        return "Hello";
    }

    [WolverineGet("/results/static")]
    public static ArithmeticResults FetchStaticResults()
    {
        return new ArithmeticResults
        {
            Sum = 3,
            Product = 4
        };
    }

    #region sample_using_string_route_parameter

    [WolverineGet("/name/{name}")]
    public static string SimpleStringRouteArgument(string name)
    {
        return $"Name is {name}";
    }

    #endregion

    #region sample_using_numeric_route_parameter

    [WolverineGet("/age/{age}")]
    public static string IntRouteArgument(int age)
    {
        return $"Age is {age}";
    }

    #endregion

    #region sample_using_string_value_as_query_string

    [WolverineGet("/querystring/string")]
    public static string UsingQueryString(string name) // name is from the query string
    {
        return name.IsEmpty() ? "Name is missing" : $"Name is {name}";
    }

    #endregion

    [WolverineGet("/querystring/int")]
    public static string UsingQueryStringParsing(Recorder recorder, int? age)
    {
        recorder.Actions.Add("got through query string usage");
        return $"Age is {age}";
    }

    [WolverineGet("/querystring/int/nullable")]
    public static string UsingQueryStringParsingNullable(int? age)
    {
        if (!age.HasValue)
        {
            return "Age is missing";
        }

        return $"Age is {age}";
    }

    #region sample_simple_wolverine_http_endpoint

    [WolverinePost("/question")]
    public static ArithmeticResults PostJson(Question question)
    {
        return new ArithmeticResults
        {
            Sum = question.One + question.Two,
            Product = question.One * question.Two
        };
    }

    #endregion

    #region sample_simple_wolverine_http_endpoint_async

    [WolverinePost("/question2")]
    public static Task<ArithmeticResults> PostJsonAsync(Question question)
    {
        var results = new ArithmeticResults
        {
            Sum = question.One + question.Two,
            Product = question.One * question.Two
        };

        return Task.FromResult(results);
    }

    #endregion

    [WolverineGet("/querystring/enum")]
    public static string UsingEnumQuerystring(Direction direction)
    {
        return direction.ToString();
    }
    
    [WolverineGet("/querystring/enum/nullable")]
    public static string UsingNullableEnumQuerystring(Direction? direction)
    {
        return direction?.ToString() ?? "none";
    }
}

public class ArithmeticResults
{
    public int Sum { get; set; }
    public int Product { get; set; }
}

public class Question
{
    public int One { get; set; }
    public int Two { get; set; }
}

public enum Direction
{
    North, East, West, South
}