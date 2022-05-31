using Azure.Storage.Files.DataLake;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Compression;


var zipname = "ziptestcontainer.zip";
var inputZipPath = Path.Combine("somepath", zipname);


using var inputStream = File.OpenRead(inputZipPath);
var stopwatch = Stopwatch.StartNew();


var datalakeClient = new DataLakeServiceClient("...");

var fileSystemClient = datalakeClient.GetFileSystemClient("ziptest");
await fileSystemClient.CreateIfNotExistsAsync();

var directoryClient = fileSystemClient.GetDirectoryClient("somefolderforzips");
await directoryClient.CreateIfNotExistsAsync();


var blockingCollection = new BlockingCollection<(Stream fileStream, string fileName)>(10);


var produceTask = Task.Run(() =>
{
    var i = 0;
    foreach (var (fileStream, fileName) in DoZipStuff.DoZipStuffAsync(inputStream))
    {
        blockingCollection.Add((fileStream, fileName));
        Console.WriteLine($"Adding file {i++} to {fileName}");
    }

    blockingCollection.CompleteAdding();
});

using var semarephore = new SemaphoreSlim(5, 5);
var tasks = new ConcurrentBag<Task>();

var consumeTask = Task.Run(async () =>
{
    var i = 0;
    while (blockingCollection.TryTake(out var file, -1))
    {
        await semarephore.WaitAsync();
        tasks.Add(Task.Run(async () =>
        {
            Console.WriteLine($"Writing file {i++} to {file.fileName}");
            var fileClient = directoryClient.GetFileClient(file.fileName);
            await fileClient.UploadAsync(file.fileStream, overwrite: true);
            await fileClient.SetMetadataAsync(new Dictionary<string, string> { { "originalzipname", zipname } });
            semarephore.Release();
        }));
    }
});


await Task.WhenAll(produceTask, consumeTask);
await Task.WhenAll(tasks);
//var i = 0;
//foreach (var (fileStream, fileName) in DoZipStuff.DoZipStuffAsync(inputStream))
//{
//    //var outputPath = Path.Combine(extractPath, fileName);
//    Console.WriteLine($"Writing file {i++} to {fileName}");

//    var fileClient = directoryClient.GetFileClient(fileName);
//    await fileClient.UploadAsync(fileStream, overwrite: true);
//    await fileClient.SetMetadataAsync(new Dictionary<string, string> { { "originalzipname", zipname } });
//    //Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);
//    //using var outputFileStream = File.OpenWrite(outputPath);
//    //await fileStream.CopyToAsync(outputFileStream);
//    //await outputFileStream.FlushAsync();
//}

Console.WriteLine(stopwatch.ElapsedMilliseconds / 1000);





public class DoZipStuff
{
    public static IEnumerable<(Stream fileStream, string fileName)> DoZipStuffAsync(Stream input)
    {
        using var archive = new ZipArchive(input);
        Console.WriteLine($"Found {archive.Entries.Count} in zip");

        foreach (ZipArchiveEntry entry in archive.Entries)
        {
            if (entry.Length > 0)
            {
                var outputStream = entry.Open();
                yield return (outputStream, entry.FullName);
            }
        }
    }
}