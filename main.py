# vim:fileencoding=utf-8:ts=4:sw=4:sts=4:expandtab

import os
import subprocess
import requests
import time
import datetime


FILE_TRANSCRIBE_STATUS_MAP = {
    'JobWaiting':                   'Transcription job waiting',
    'DownloadMedia':                'Downloading media file',
    'DownloadMediaFailed':          'Download media failed',
    'DownloadMediaFinished':        'Download media finished',
    'ProbeMedia':                   'Probing media file',
    'ProbeMediaFailed':             'Probe media failed',
    'ProbeMediaFinished':           'Probe media finished',
    'ExtractAudio':                 'Extracting audio from video',
    'ExtractAudioFailed':           'Extract audio from video failed',
    'ExtractAudioFinished':         'Extract audio from video finished',
    'TranscribeAudio':              'Transcription in progress',
    'TranscribeAudioFailed':        'Transcription failed',
    'TranscribeAudioFinished':      'Transcription finished',
    'UploadTranscription':          'Uploading transcription text',
    'UploadTranscriptionFailed':    'Upload transcription text failed',
    'UploadTranscriptionFinished':  'Upload transcription text finished',
    'TextStorageQueued':            'Update transcribe record text queued',
    'TextStorageFailed':            'Update transcribe record text failed',
    'TextStorageFinished':          'Update transcribe record text finished',
    'TranscriptionComplete':        'Audio transcription complete',
}

# Get the value of an environment variable
URL = os.environ.get('URL')
SECRET_KEY = os.environ.get('SECRET_KEY')

params = {'SecretKey': SECRET_KEY}

def GetJob():
    try:
        # get the next job
        resp = requests.get(URL + '/GetJob', params=params)
        resp.raise_for_status()
        job = resp.json()

        return job
    except requests.exceptions.HTTPError as err:
        print(err)
        return None

def PostJobStatus(JobID, Status, *, Error=None):
    try:
        print(f'Job status changed to `' + FILE_TRANSCRIBE_STATUS_MAP.get(Status, '[Status Unkown]') + '`' + (' with error: ' + Error if Error else ''))
        # post the job status
        resp = requests.post(URL + '/PostJobStatus', params=params, json={'JobID': JobID, 'SecretKey': SECRET_KEY, 'Status': Status, 'Error': Error})
        resp.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(err)



while True:

    print('______________________________________________________')
    print('Starting a new iteration on ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '...')

    JobID = None
    Download_URL = None
    Upload_URL = None

    job = GetJob()

    if job is None:
        print('No job returned. Sleeping for 10 seconds...')
        time.sleep(10)
        continue

    JobID = job.get('JobID')
    Download_URL = job.get('Download_URL')
    Upload_URL = job.get('Upload_URL')

    if Download_URL is None:
        PostJobStatus(JobID, 'DownloadMediaFailed', Error='No download URL specified')
        print('Sleeping for 10 seconds...')
        time.sleep(10)
        continue

    FileName_Base = f'transcribe_{JobID}'
    FileName_Audio = f'{FileName_Base}'

    print(f'JobID: {JobID}')
    print(f'Download_URL: {Download_URL}')
    print(f'Upload_URL: {Upload_URL}')
    
    print('')

    try:
        PostJobStatus(JobID, 'DownloadMedia', Error=None)

        # download the file
        resp = requests.get(Download_URL)
        resp.raise_for_status()

        with open('mediafile', 'wb') as file:
            file.write(resp.content)
        
        PostJobStatus(JobID, 'DownloadMediaFinished', Error=None)
    except requests.exceptions.HTTPError as err:
        PostJobStatus(JobID, 'DownloadMediaFailed', Error=str(err))
        continue

    try:
        PostJobStatus(JobID, 'ProbeMedia', Error=None)
        
        # check the file's content type using ffprobe
        command = "ffprobe -v error -show_entries stream=codec_type -of default=noprint_wrappers=1 mediafile"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        PostJobStatus(JobID, 'ProbeMediaFinished', Error=None)
    except subprocess.CalledProcessError as err:
        PostJobStatus(JobID, 'ProbeMediaFailed', Error=str(err) + ': ' + str(err.output))
        continue

    
    # If the output contains 'video', it's a video file
    if 'video' in result.stdout:
        print('It is a video file.')

        # update the audio file name - ffmpeg is picky about the file extension
        FileName_Audio = FileName_Audio + '.wav'       
        
        try:
            PostJobStatus(JobID, 'ExtractAudio', Error=None)

            # Extract audio using ffmpeg, then clean up the video file
            command = f"ffmpeg -i mediafile -vn -acodec pcm_s16le -ar 44100 -ac 2 {FileName_Audio}"
            result = subprocess.run(command, shell=True, capture_output=True, text=True)

            if result.returncode != 0:
                error_output = result.stderr.strip()
                raise subprocess.CalledProcessError(result.returncode, 'ffmpeg', error_output)
            
            PostJobStatus(JobID, 'ExtractAudioFinished', Error=None)
        except subprocess.CalledProcessError as err:
            PostJobStatus(JobID, 'ExtractAudioFailed', Error=str(err) + ': ' + str(err.output))
            continue
    
    elif 'audio' in result.stdout:
        print('It is an audio file.')

        # just rename it
        os.rename('mediafile', FileName_Audio)

        print('Renamed the audio file')
    # .m4a files seem to have subtitle stream, and are mp4 files without video stream
    elif 'subtitle' in result.stdout:
        print('It is a m4a file with subtitle stream.')

        # just rename it
        os.rename('mediafile', FileName_Audio)

        print('Renamed the subtitle file')
    else:
        PostJobStatus(JobID, 'TranscribeAudioFailed', Error='Unknown file type: "' + str(result.stdout) + '"')
        continue
    
    
    try:
        PostJobStatus(JobID, 'TranscribeAudio', Error=None)

        # transcribe the audio
        command = f"whisper {FileName_Audio} --model small --language en --output_format vtt"
        result = subprocess.run(command, shell=True, capture_output=True, text=True, env={"TORCH_HOME":"/work/.whisper_cache"})
        if result.returncode != 0:
            error_output = result.stderr.strip()
            raise subprocess.CalledProcessError(result.returncode, 'whisper', error_output)

        PostJobStatus(JobID, 'TranscribeAudioFinished', Error=None)
    except subprocess.CalledProcessError as err:
        PostJobStatus(JobID, 'TranscribeAudioFailed', Error=str(err) + ': ' + str(err.output))
        continue

    try:
        PostJobStatus(JobID, 'UploadTranscription', Error=None)

        #read the file and upload it to s3 using the signed url
        with open(f'{FileName_Base}.vtt', 'rb') as file:
            resp = requests.put(Upload_URL['UploadURL'], data=file)
            resp.raise_for_status()
        
        PostJobStatus(JobID, 'UploadTranscriptionFinished', Error=None)
    except requests.exceptions.HTTPError as err:
        PostJobStatus(JobID, 'UploadTranscriptionFailed', Error=str(err))
        continue
    
    try:
        print('Cleaning up...')
        subprocess.call("rm -rf /work/*", shell=True)

    except FileNotFoundError:
        pass

    print('Sleeping for 10 seconds...')
    time.sleep(10)
