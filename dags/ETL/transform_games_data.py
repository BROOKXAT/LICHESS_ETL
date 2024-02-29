import zstandard as zstd 
import chess.pgn
import pandas as pd
import os
import psycopg2

def decompress_zst_file(file_path):
    with open(file_path, 'rb') as compressed_file:
        decompressor = zstd.ZstdDecompressor()
        with decompressor.stream_reader(compressed_file) as reader:
            decompressed_data = reader.read()
        with open(file_path[:-4],'wb') as file :
                file.write(decompressed_data)
#    return decompressed_data

#decompressed = decomress_zst_file('C:\\Users\\hp\\Desktop\\Docker-containers\\Lichess_ETL\\ETL\\temp_lichess_data\\lichess_db_standard_rated_2013-01.pgn.zst')
#with open('C:\\Users\\hp\\Desktop\\Docker-containers\\Lichess_ETL\\ETL\\temp_lichess_data\\decompressedlichess_db_standard_rated_2013-01.pgn','wb') as file :
#                file.write(decompressed)

def parse_pgn_file(file_path) :
        # Add your PostgreSQL connection details
        db_params = {
            'dbname': 'lichess_db',
            'user': 'airflow',
            'password': 'airflow',
            'host': 'postgres',
            'port': '5432',
        }
        conn = psycopg2.connect(**db_params)

        games = []
        counter = 0 
        with open(file_path , "r") as pgn_file :
              while True :
                     game = chess.pgn.read_game(pgn_file)
                     if game is None : break
                     #if counter == 100 : break
                     game_data = {
                            "Event" : game.headers["Event"] ,
                            "Site" : game.headers["Site"] ,
                            "White" : game.headers["White"] ,
                            "Black" : game.headers["Black"] ,
                            "Date" : game.headers["UTCDate"] ,
                            "Time" : game.headers["UTCTime"] ,
                            "WhiteElo" : game.headers["WhiteElo"] ,
                            "BlackElo" : game.headers["BlackElo"] ,
                            "Opening" : game.headers["Opening"] ,
                            "TimeControl" : game.headers["TimeControl"] ,
                            "Termination" : game.headers["Termination"] ,
                            "Result" : game.headers["Result"] ,
                            #"moves" : list(game.mainline_moves()), 
                     }

                     games.append(game_data)
                     insert_into_postgres(game_data, conn)
                     #counter += 1
        df = pd.DataFrame(games)
        csv_file_path = file_path.replace(".pgn", ".csv")
        df.to_csv(csv_file_path, index=False)
        conn.close()

                    
#games =parse_pgn_file('C:\\Users\\hp\\Desktop\\Docker-containers\\Lichess_ETL\\ETL\\temp_lichess_data\\decompressedlichess_db_standard_rated_2013-01.pgn')
#print(pd.DataFrame(games))

def delete_file(file_path):
    os.remove(file_path)

def insert_into_postgres(game_data, conn):
    try:
        cursor = conn.cursor()
        white_elo = int(game_data["WhiteElo"]) if game_data["WhiteElo"].isdigit() else None
        black_elo = int(game_data["BlackElo"]) if game_data["BlackElo"].isdigit() else None
        # Construct the SQL INSERT statement
        insert_query = """
        INSERT INTO games
        (event, site, white, black, date, time, white_elo, black_elo, opening, time_control, termination, result)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Execute the INSERT statement
        cursor.execute(insert_query, (
            game_data["Event"],
            game_data["Site"],
            game_data["White"],
            game_data["Black"],
            game_data["Date"],
            game_data["Time"],
            white_elo,
            black_elo,
            game_data["Opening"],
            game_data["TimeControl"],
            game_data["Termination"],
            game_data["Result"],
        ))

        # Commit the changes
        conn.commit()

    except Exception as e:
        print(f"Error inserting data into PostgreSQL: {e}")