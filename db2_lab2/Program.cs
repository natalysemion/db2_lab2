using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks;

internal class Program
{
    private static async Task Main(string[] args)
    {
        var mongoDatabaseName = "MusicDB_2";
        var mongoConnectionString = "mongodb://localhost:27017";
        var mssqlConnectionString = "Data Source=DESKTOP-1PBP35V\\SQLEXPRESS;Initial Catalog=TicketsMarket;Trusted_Connection=True;";

        var mongoClient = new MongoClient(mongoConnectionString);
        var dbMongo = mongoClient.GetDatabase(mongoDatabaseName);

        var artistsCollection = dbMongo.GetCollection<BsonDocument>("Artists");
        var songsCollection = dbMongo.GetCollection<BsonDocument>("Songs");
        var albumsCollection = dbMongo.GetCollection<BsonDocument>("Albums");
        var labelsCollection = dbMongo.GetCollection<BsonDocument>("Labels");

        var random = new Random();
        int N = 100;

        var mongodbStopwatch4 = new Stopwatch();
        mongodbStopwatch4.Start();
        await artistsCollection.DeleteManyAsync(new BsonDocument());
        mongodbStopwatch4.Stop();
     
        var mongodbStopwatch1 = new Stopwatch();
        mongodbStopwatch1.Start();

        for (int i = 0; i < N; i++)
        {
            // Insert artist
            var artistDocument = new BsonDocument()
                    {
                        {"name", $"Artist{i}"},
                        {"birth_date", DateTime.Now},
                        {"country_id", random.Next(1, 5)},
                        {"spotify_id", $"SpotifyId{i}"},
                        {"label_id", random.Next(1, 5)}
                    };
            await artistsCollection.InsertOneAsync(artistDocument);
        }
        mongodbStopwatch1.Stop();
        Console.WriteLine($"It takes {mongodbStopwatch1.ElapsedMilliseconds} milliseconds to execute INSERT operation in MongoDB");

        var mongodbStopwatch2 = new Stopwatch();
        mongodbStopwatch2.Start();
        for (int i = 0; i < N; i++)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("label_id", 2);
            var update = Builders<BsonDocument>.Update.Set("name", "updateArtist");
            await artistsCollection.UpdateManyAsync(filter, update);

        }
        mongodbStopwatch2.Stop();
        Console.WriteLine($"It takes {mongodbStopwatch2.ElapsedMilliseconds} milliseconds to execute UPDATE operation in MongoDB");

        var mongodbStopwatch3 = new Stopwatch();
        mongodbStopwatch3.Start();
        for (int i = 0; i < N; i++)
        {
            var filter = Builders<BsonDocument>.Filter.Eq("name", "Artist{i}");

            await artistsCollection.DeleteManyAsync(filter);
        }
        mongodbStopwatch3.Stop();
        Console.WriteLine($"It takes {mongodbStopwatch3.ElapsedMilliseconds} milliseconds to execute DELETE operation in MongoDB");


        var mongodbStopwatch5 = new Stopwatch();
        mongodbStopwatch5.Start();
        await artistsCollection.DeleteManyAsync(new BsonDocument());

        mongodbStopwatch5.Stop();
    

        var mssqlStopwatch1 = new Stopwatch();
        var mssqlStopwatch2 = new Stopwatch();
        var mssqlStopwatch3 = new Stopwatch();

        using (SqlConnection connection = new SqlConnection(mssqlConnectionString))
        {
            try
            {

                await connection.OpenAsync();
         
                var sqlDeleteAllArtistsQuery = "DELETE FROM [dbo].[Artist]";

                using (SqlCommand command = new SqlCommand(sqlDeleteAllArtistsQuery, connection))
                {
                    await command.ExecuteNonQueryAsync();
                }

                mssqlStopwatch1.Start();

                for (int i = 10; i < N; i++)
                {
                    var sqlCreateArtistQuery = $"INSERT INTO[dbo].[Artist]([id], [name], [birth_date], [country_id], [spotify_id], [label_id])" +
                            $"VALUES({i}, 'ArtistName', '2023-01-01', 1, 'SpotifyId123', 2)";


                    using (SqlCommand command = new SqlCommand(sqlCreateArtistQuery, connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                }
                mssqlStopwatch1.Stop();
                Console.WriteLine($"It takes {mssqlStopwatch1.ElapsedMilliseconds} milliseconds to execute INSERT operation in MSSQL");
                mssqlStopwatch2.Start();
                for (int i = 0; i < N; i++)
                {
                    var sqlUpdateArtistQuery = $"UPDATE [dbo].[Artist] " +
                          $"SET [name] = 'updateArtist' " +
                          $"WHERE [label_id] = 2";

                    using (SqlCommand command = new SqlCommand(sqlUpdateArtistQuery, connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                }
                mssqlStopwatch2.Stop();
                Console.WriteLine($"It takes {mssqlStopwatch2.ElapsedMilliseconds} milliseconds to execute UPDATE operation in MSSQL");


                mssqlStopwatch3.Start();
                for (int i = 0; i < N; i++)
                {
                    var sqlDeleteArtistQuery = $"DELETE FROM [dbo].[Artist] " +
                               $"WHERE [name] = 'updateArtist'";

                    using (SqlCommand command = new SqlCommand(sqlDeleteArtistQuery, connection))
                    {
                        await command.ExecuteNonQueryAsync();
                    }
                }
                mssqlStopwatch3.Stop();
                Console.WriteLine($"It takes {mssqlStopwatch3.ElapsedMilliseconds} milliseconds to execute DELETE operation in MSSQL");
                connection.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
            
        }
    }
}

