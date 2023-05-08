import base64
import concurrent.futures
import csv

import requests


class SpotifyAPI:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None

    def get_token(self):
        if self.token is not None:
            return self.token

        token_url = 'https://accounts.spotify.com/api/token'

        client_creds = f'{self.client_id}:{self.client_secret}'
        encoded_creds = base64.b64encode(client_creds.encode()).decode()

        headers = {
            'Authorization': f'Basic {encoded_creds}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            'grant_type': 'client_credentials'
        }

        # Send the POST request to the token URL with the headers and data
        response = requests.post(token_url, headers=headers, data=data)

        # Extract the access token from the response
        access_token = response.json()['access_token']
        self.token = access_token

        return access_token

    def search_track(self, track_name, artist_name):
        query = f"track:{track_name} artist:{artist_name}"
        endpoint = 'https://api.spotify.com/v1/search'
        headers = {'Authorization': f'Bearer {self.get_token()}'}

        response = requests.get(endpoint, headers=headers, params={'q': query, 'type': 'track'})

        if response.status_code != 200:
            raise Exception(f"Error retrieving image URL for {track_name} by {artist_name}: {response.text}")

        try:
            image_url = response.json()['tracks']['items'][0]['album']['images'][0]['url']
        except (KeyError, IndexError):
            image_url = None

        return image_url


class CSVProcessor:
    def __init__(self, input_file_name, output_file_name):
        self.csvwriter = None
        self.failure_count = 0
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name

    def process_row(self, row):
        spotify_api = SpotifyAPI(CLIENT_ID, CLIENT_SECRET)
        new_row = row.copy()
        try:
            # print(f"{row[3]}, {row[1]}: ", end="")
            url = spotify_api.search_track(row[3], row[1])
            if url is None:
                url = spotify_api.search_track(row[3], row[1])
            print(url)
            new_row.append(url)
        except Exception as e:
            new_row.append('')
            print(f"Error: {e}")
            self.failure_count += 1
        self.csvwriter.writerow(new_row)

    def process(self):
        with open(self.input_file_name, encoding='utf8') as input_file, \
                open(self.output_file_name, 'w', encoding='utf8') as output_file:
            csvreader = csv.reader(input_file)
            self.csvwriter = csv.writer(output_file)

            header = next(csvreader)
            header.append('Cover_image_url')
            self.csvwriter.writerow(header)

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = []

                for i, row in enumerate(csvreader):
                    futures.append(executor.submit(self.process_row, row))

                for future in futures:
                    future.result()

            print(f"Total failures: {self.failure_count}")


if __name__ == '__main__':
    CLIENT_ID = '92d1e28654cb4905a2d981f2d590b247'
    CLIENT_SECRET = 'c7a5688bed764d3b9874ac88492fad36'
    CSV_FILE_NAME = 'musics_dataset.csv'
    UPDATED_CSV_FILE_NAME = 'musics_dataset_updated.csv'

    csv_processor = CSVProcessor(CSV_FILE_NAME, UPDATED_CSV_FILE_NAME)
    csv_processor.process()
