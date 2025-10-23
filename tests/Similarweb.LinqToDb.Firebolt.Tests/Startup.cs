using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Similarweb.LinqToDB.Firebolt.Tests.Common;
using Similarweb.LinqToDB.Firebolt.Tests.Options;

namespace Similarweb.LinqToDB.Firebolt.Tests;

public class Startup
{
    public void ConfigureServices(IServiceCollection services)
    {
        // TODO: Chase Firebolt and check that data is returned for cloud version in same way as for Core version.
        // Currently for Core numeric values are returned as a string with dot (`.`) as decimal separator. So we need
        // this code to be sure that tests are passing.
        // Obviously, this is safe only in case you will be using SDK against cloud version OR have locale set to `EN-us`
        // or something like this, with dot (`.`) as decimal separator
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
        CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;

        var envType = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? string.Empty;

        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("testsettings.json", optional: true, reloadOnChange: true)
            .AddJsonFile($"testsettings.{envType}.json", optional: true, reloadOnChange: true)
            .AddJsonFile($"testsettings.local.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .AddCommandLine(Environment.GetCommandLineArgs())
            .Build();

        services
            .Configure<LinqToDbTestSettings>(configuration.GetSection(nameof(LinqToDbTestSettings)))
            .Configure<Dictionary<string, Dictionary<string, string>>>(configuration.GetSection("accounts"))
            .AddSingleton<ConnectionStringsProvider>();
    }
}

