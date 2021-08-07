using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace crawler
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            var concurrent = 4;
            var cancellationTokenSource = new CancellationTokenSource();

            await foreach (var item in Crawl(
                "https://golang.org/",
                concurrent,
                cancellationTokenSource.Token
            ).WithCancellation(cancellationTokenSource.Token))
            {
                var timeStamp = item.Timestamp.ToString("O");
                Console.WriteLine(
                    $"[{item.Name}] ({timeStamp}) {item.Url} => {(item.Exception != null ? item.Exception.Message : item.Body)}"
                );
            }
        }

        private static async IAsyncEnumerable<(DateTime Timestamp, string Name, string Url, string? Body, Exception?
            Exception)> Crawl(
            string url, int concurrent,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            IDictionary<string, bool> visitedUrl = new ConcurrentDictionary<string, bool>();

            var urlChanel = Channel.CreateBounded<string?>(concurrent);
            await urlChanel.Writer.WriteAsync(url, cancellationToken).ConfigureAwait(false);

            var urlsChanel = Channel.CreateBounded<IEnumerable<string>>(concurrent);

            var result = Channel
                .CreateBounded<(DateTime Timestamp, string Name, string Url, string? Body, Exception? Exception)>(
                    concurrent);

            var urlQueue = new Queue<string>();
            var counter = 1;

            var tasks = Enumerable.Range(1, concurrent).Select(async index =>
            {
                var worker = $"Worker:{index}";

                while (true)
                {
                    var currentUrl = await urlChanel.Reader.ReadAsync(cancellationToken);

                    if (currentUrl == null) break;

                    var timestamp = DateTime.Now;

                    try
                    {
                        visitedUrl.Add(currentUrl, true);

                        var fetchResult = await FetchAsync(currentUrl, cancellationToken).ConfigureAwait(false);

                        await urlsChanel.Writer
                            .WriteAsync(fetchResult.urls.Where(c => !visitedUrl.ContainsKey(c)), cancellationToken)
                            .ConfigureAwait(false);

                        await result.Writer.WriteAsync((timestamp, worker, currentUrl, fetchResult.body, null),
                                cancellationToken)
                            .ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        await urlsChanel.Writer
                            .WriteAsync(Enumerable.Empty<string>(), cancellationToken)
                            .ConfigureAwait(false);

                        await result.Writer.WriteAsync((timestamp, worker, currentUrl, null, ex), cancellationToken)
                            .ConfigureAwait(false);
                    }
                }
            }).ToArray();

            while (!cancellationToken.IsCancellationRequested)
            {
                var item = await result.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);

                yield return item;

                counter--;

                foreach (var item2 in await urlsChanel.Reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    urlQueue.Enqueue(item2);

                if (counter == 0 && !urlQueue.Any())
                    yield break;

                for (; counter < concurrent && urlQueue.Count > 0; counter++)
                {
                    var node = urlQueue.Dequeue();
                    await urlChanel.Writer.WriteAsync(node, cancellationToken).ConfigureAwait(false);
                }
            }

            await Task.WhenAll(Enumerable.Repeat<string?>(null, concurrent).Select(async c =>
                await urlChanel.Writer.WriteAsync(c, cancellationToken).ConfigureAwait(false))).ConfigureAwait(false);
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        private static async Task<(string body, IEnumerable<string> urls)> FetchAsync(string url,
            CancellationToken cancellationToken = default)
        {
            if (!Fetcher.TryGetValue(url, out var result))
            {
                await Task.Delay(500, cancellationToken).ConfigureAwait(false);
                throw new Exception("Not found");
            }

            await Task.Delay(result.elapsed, cancellationToken).ConfigureAwait(false);
            return (result.body, result.urls);
        }

        private static readonly IDictionary<string, (string body, int elapsed, IEnumerable<string> urls)> Fetcher =
            new Dictionary<string, (string, int, IEnumerable<string>)>
            {
                {
                    "https://golang.org/", (
                        "The Go Programming Language", 1000, new[]
                        {
                            "https://golang.org/pkg/",
                            "https://golang.org/cmd/",
                            "https://golang.org/internal/"
                        })
                },
                {
                    "https://golang.org/internal/", (
                        "Packages internal", 1000, new[]
                        {
                            "https://golang.org/"
                        })
                },
                {
                    "https://golang.org/pkg/", (
                        "Packages", 1000, new[]
                        {
                            "https://golang.org/",
                            "https://golang.org/cmd/",
                            "https://golang.org/pkg/fmt/",
                            "https://golang.org/pkg/os/",
                            "https://golang.org/pkg/container/",
                        })
                },
                {
                    "https://golang.org/pkg/fmt/", (
                        "Package fmt", 1000, new[]
                        {
                            "https://golang.org/",
                            "https://golang.org/pkg/"
                        })
                },
                {
                    "https://golang.org/pkg/container/", (
                        "Package container", 1000, new[]
                        {
                            "https://golang.org/pkg/",
                            "https://golang.org/pkg/container/list/",
                            "https://golang.org/pkg/container/heap/",
                        })
                },
                {
                    "https://golang.org/pkg/container/list/", (
                        "Package list", 1000, new[]
                        {
                            "https://golang.org/pkg/container/"
                        })
                },
                {
                    "https://golang.org/pkg/container/heap/", (
                        "Package heap", 1000, new[]
                        {
                            "https://golang.org/pkg/container/"
                        })
                },
                {
                    "https://golang.org/pkg/os/", (
                        "Package os", 1000, new[]
                        {
                            "https://golang.org/",
                            "https://golang.org/pkg/"
                        })
                },
            };
    }
}