import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import asyncio
import aiohttp
import aiofiles
from zipfile import ZipFile
from datetime import datetime, timedelta

scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)
print("Google Sheets API client successfully authorized.")

spreadsheet_id = '1CQS8bEXS6O0HljtE6Sx5QLPKzsoEJC2APthWuvMvKn0'
spreadsheet = client.open_by_key(spreadsheet_id)
print(f"Spreadsheet with ID {spreadsheet_id} successfully opened.")

def get_next_date_worksheet():
    today = datetime.today().date()
    nearest_sheet = None
    min_diff = timedelta(days=365 * 100)
    print("Searching for the nearest future date worksheet...")
    for sheet in spreadsheet.worksheets():
        try:
            sheet_date = datetime.strptime(sheet.title, '%d/%m/%Y').date()
            diff = sheet_date - today
            if diff.days >= 0 and diff < min_diff:
                min_diff = diff
                nearest_sheet = sheet
                print(f"Found a worksheet: {sheet.title} for date {sheet_date}")
        except ValueError:
            continue
    if not nearest_sheet:
        raise ValueError("No suitable worksheet found.")
    print(f"Nearest worksheet selected: {nearest_sheet.title}")
    return nearest_sheet

worksheet = get_next_date_worksheet()

def get_urls_from_sheet():
    ranges = ["F4:F9", "L4:L16", "R4:R12"]
    urls = []
    print("Retrieving URLs from the worksheet...")
    for range_ in ranges:
        cell_list = worksheet.range(range_)
        for cell in cell_list:
            if cell.value.strip():
                urls.append(cell.value)
    print(f"Total URLs retrieved: {len(urls)}")
    return urls

async def download_file(session, url):
    try:
        print(f"Starting download: {url}")
        async with session.get(url) as response:
            response.raise_for_status()
            filename = url.split('/')[-1]
            save_path = os.path.join("downloads", filename)
            async with aiofiles.open(save_path, 'wb') as file:
                content = await response.read()
                await file.write(content)
            print(f"Downloaded: {filename}")
            return save_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

async def download_files_and_zip(urls, zip_filename):
    if not os.path.exists("downloads"):
        os.makedirs("downloads")
        print("Created downloads directory.")
    async with aiohttp.ClientSession() as session:
        tasks = [download_file(session, url) for url in urls if url]
        file_paths = await asyncio.gather(*tasks)
        with ZipFile(zip_filename, 'w') as zip_file:
            for file_path in file_paths:
                if file_path:
                    zip_file.write(file_path, os.path.basename(file_path))
                    os.remove(file_path)
                    print(f"Added {os.path.basename(file_path)} to zip and removed the original file.")
    print(f"All files have been downloaded and zipped into {zip_filename}")

async def main():
    urls = get_urls_from_sheet()
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"trackmania_maps_{current_time}.zip"
    print(f"Starting the download and zip process at {current_time}.")
    await download_files_and_zip(urls, zip_filename)

if __name__ == '__main__':
    asyncio.run(main())
