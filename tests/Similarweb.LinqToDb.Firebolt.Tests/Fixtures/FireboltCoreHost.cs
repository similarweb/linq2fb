using Docker.DotNet.Models;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Similarweb.LinqToDB.Firebolt.Tests.Options;
using Similarweb.LinqToDB.Firebolt.Tests.Seeding;

namespace Similarweb.LinqToDB.Firebolt.Tests.Fixtures;

/// <summary>
/// Starts Firebolt Core via Testcontainers (or uses an external URL), seeds Northwind once per test host.
/// Startup is lazy: the container is created on first Core connection-string resolution so mocked-only
/// test runs do not require Docker.
/// </summary>
/// <remarks>
/// Configure image/ports in <c>testsettings.json</c> (<c>fireboltCore</c>).
/// Set <c>fireboltCore.externalUrl</c> (or env <c>FireboltCore__ExternalUrl</c>) to attach to a
/// pre-started Core instance; the database must already be seeded in that case.
/// </remarks>
public sealed class FireboltCoreHost : IHostedService, IAsyncDisposable
{
    private const long MemlockLimit = 8589934592;

    private readonly FireboltCoreSettings _settings;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IContainer? _container;
    private string? _baseUrl;
    private bool _started;
    private bool _ownsContainer;
    private int _disposed;

    public FireboltCoreHost(IOptions<FireboltCoreSettings> settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        _settings = settings.Value;
        ArgumentException.ThrowIfNullOrWhiteSpace(_settings.Image);
    }

    /// <summary>
    /// Base URL of the running Core instance. Starts and seeds the container on first access when needed.
    /// </summary>
    public string BaseUrl
    {
        get
        {
            if (_baseUrl is not null)
            {
                return _baseUrl;
            }

            // Test fixtures resolve connection strings synchronously; start once on first Core use.
            EnsureStartedAsync().ConfigureAwait(false).GetAwaiter().GetResult();
            return _baseUrl!;
        }
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => DisposeAsync().AsTask();

    public async Task EnsureStartedAsync(CancellationToken cancellationToken = default)
    {
        if (_started)
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_started)
            {
                return;
            }

            if (!string.IsNullOrWhiteSpace(_settings.ExternalUrl))
            {
                _baseUrl = NormalizeBaseUrl(_settings.ExternalUrl);
                _ownsContainer = false;
                _started = true;
                Console.WriteLine($"Using external Firebolt Core at {_baseUrl}");
                return;
            }

            var image = _settings.Image;
            var httpPort = _settings.HttpPort;
            var secondaryPort = _settings.SecondaryPort;
            Console.WriteLine($"Starting Firebolt Core Testcontainers image '{image}'…");

            _container = new ContainerBuilder(image)
                .WithPortBinding(httpPort, true)
                .WithPortBinding(secondaryPort, true)
                .WithCreateParameterModifier(ApplyHostRequirements)
                .WithWaitStrategy(
                    Wait.ForUnixContainer()
                        .UntilCommandIsCompleted("fbcli", "--command", "SELECT 42;"))
                .Build();

            await _container.StartAsync(cancellationToken).ConfigureAwait(false);

            var mappedPort = _container.GetMappedPublicPort(httpPort);
            _baseUrl = $"http://{_container.Hostname}:{mappedPort}/";
            _ownsContainer = true;

            Console.WriteLine($"Firebolt Core is ready at {_baseUrl}");

            var version = await TryGetVersionAsync(_container, cancellationToken).ConfigureAwait(false);
            if (version is not null)
            {
                Console.WriteLine($"Firebolt Core version: {version}");
            }

            var sqlPath = Path.Combine(AppContext.BaseDirectory, "sql", "northwind.sql");
            await NorthwindSeeder.SeedAsync(_container, sqlPath, cancellationToken).ConfigureAwait(false);

            _started = true;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        if (_ownsContainer && _container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
            _container = null;
            _ownsContainer = false;
        }

        _gate.Dispose();
        GC.SuppressFinalize(this);
    }

    private static void ApplyHostRequirements(CreateContainerParameters parameters)
    {
        parameters.HostConfig ??= new HostConfig();
        parameters.HostConfig.SecurityOpt = ["seccomp=unconfined"];
        parameters.HostConfig.Ulimits =
        [
            new Ulimit
            {
                Name = "memlock",
                Soft = MemlockLimit,
                Hard = MemlockLimit,
            },
        ];
    }

    private static string NormalizeBaseUrl(string url)
    {
        url = url.Trim();
        return url.EndsWith('/') ? url : url + "/";
    }

    private static async Task<string?> TryGetVersionAsync(IContainer container, CancellationToken cancellationToken)
    {
        try
        {
            var result = await container
                .ExecAsync(["fbcli", "--format", "JSONLines_Compact", "--", "SELECT VERSION()"], cancellationToken)
                .ConfigureAwait(false);
            if (result.ExitCode != 0)
            {
                return null;
            }

            // fbcli prints a URL line then JSON; keep raw output for diagnostics.
            var output = string.IsNullOrWhiteSpace(result.Stdout) ? result.Stderr : result.Stdout;
            return string.IsNullOrWhiteSpace(output) ? null : output.Trim();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Could not read Firebolt Core version: {ex.Message}");
            return null;
        }
    }
}
