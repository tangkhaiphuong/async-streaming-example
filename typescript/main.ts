async function main(): Promise<void> {
    const concurrent = 4;
    let isCancel = false;

    for await (const item of crawl(
        "https://golang.org/",
        concurrent,
        () => isCancel
    )) {
        console.error(
            `[${item.name}] (${item.timestamp.toISOString()}) ${item.url} => ${
                item.error?.message || item.body
            }`
        );
    }
}

interface FetchResult {
    timestamp: Date;
    name: string;
    url: string;
    body?: string;
    error?: Error;
}

async function* crawl(
    url: string,
    concurrent: number,
    isCancel: () => boolean
): AsyncGenerator<FetchResult> {
    class Channel<T> {
        private _queue: T[] = [];
        private readonly _capacity: number;

        constructor(capacity: number) {
            this._capacity = capacity;
        }

        public async read(
            isCancel?: (() => boolean) | undefined
        ): Promise<T | undefined> {
            while (this._queue.length == 0) {
                if (!!isCancel && isCancel()) return undefined;
                await new Promise((resolve) => setTimeout(resolve, 0));
            }
            return this._queue.shift()!;
        }

        public async write(
            value: T,
            isCancel?: (() => boolean) | undefined
        ): Promise<boolean> {
            while (this._queue.length >= this._capacity) {
                if (!!isCancel && isCancel()) return false;
                await new Promise((resolve) => setTimeout(resolve, 0));
            }
            this._queue.push(value);
            return true;
        }
    }

    const visitedUrl = new Set();

    const urlChanel = new Channel<string | null>(concurrent);
    const urlsChanel = new Channel<string[]>(concurrent);
    const resultChanel = new Channel<FetchResult>(concurrent);

    const urlQueue: string[] = [];

    let counter = 1;
    await urlChanel.write(url, isCancel);

    const tasks: Promise<void>[] = [];

    async function worker(name: string): Promise<void> {
        while (true) {
            const url = await urlChanel.read(isCancel);
            if (!url) break;

            const timestamp = new Date();
            try {
                visitedUrl.add(url);

                const result = await fetch(url);

                const urls: string[] = [];

                for (const item of result.urls)
                    if (!visitedUrl.has(item)) urls.push(item);

                await urlsChanel.write(urls, isCancel);

                await resultChanel.write(
                    {
                        timestamp: timestamp,
                        name: name,
                        url: url,
                        body: result.body,
                    },
                    isCancel
                );
            } catch (err) {
                await urlsChanel.write([], isCancel);

                await resultChanel.write(
                    {
                        timestamp: timestamp,
                        name: name,
                        url: url,
                        error: err as Error,
                    },
                    isCancel
                );
            }
        }
    }

    for (let index = 0; index < concurrent; index++) {
        tasks[index] = worker(`Worker:${index + 1}`);
    }

    while (true) {
        const item = await resultChanel.read(isCancel);

        if (!item) break;

        yield item;

        counter--;

        const urls = await urlsChanel.read(isCancel);

        if (urls === undefined) break;

        for (const url of urls) urlQueue.push(url);

        if (counter == 0 && urlQueue.length === 0) break;

        for (; counter < concurrent && urlQueue.length > 0; counter++) {
            const url = urlQueue.shift()!;
            await urlChanel.write(url);
        }
    }

    for (let index = 0; index < concurrent; index++) {
        await urlChanel.write(null, isCancel);
    }
    await Promise.all(tasks);
}

async function fetch(url: string): Promise<{ body: string; urls: string[] }> {
    const result = fetcher[url];
    if (!!result) {
        await new Promise((resolve) => setTimeout(resolve, result.elapsed));
        return result;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
    throw new Error("Not found");
}

const fetcher: {
    [url: string]: { body: string; elapsed: number; urls: string[] };
} = {
    "https://golang.org/": {
        body: "The Go Programming Language",
        elapsed: 1000,
        urls: [
            "https://golang.org/pkg/",
            "https://golang.org/cmd/",
            "https://golang.org/internal/",
        ],
    },

    "https://golang.org/internal/": {
        body: "Packages internal",
        elapsed: 1000,
        urls: ["https://golang.org/"],
    },
    "https://golang.org/pkg/": {
        body: "Packages",
        elapsed: 1000,
        urls: [
            "https://golang.org/",
            "https://golang.org/cmd/",
            "https://golang.org/pkg/fmt/",
            "https://golang.org/pkg/os/",
            "https://golang.org/pkg/container/",
        ],
    },
    "https://golang.org/pkg/fmt/": {
        body: "Package fmt",
        elapsed: 1000,
        urls: ["https://golang.org/", "https://golang.org/pkg/"],
    },
    "https://golang.org/pkg/container/": {
        body: "Package container",
        elapsed: 1000,
        urls: [
            "https://golang.org/pkg/",
            "https://golang.org/pkg/container/list/",
            "https://golang.org/pkg/container/heap/",
        ],
    },
    "https://golang.org/pkg/container/list/": {
        body: "Package list",
        elapsed: 1000,
        urls: ["https://golang.org/pkg/container/"],
    },
    "https://golang.org/pkg/container/heap/": {
        body: "Package heap",
        elapsed: 1000,
        urls: ["https://golang.org/pkg/container/"],
    },
    "https://golang.org/pkg/os/": {
        body: "Package os",
        elapsed: 1000,
        urls: ["https://golang.org/", "https://golang.org/pkg/"],
    },
};

main();
