"""Script to generate a dataset of metadata of all photos in a directory tree.

The dataset can be output as either a CSV file or SQLite database, each with the following columns:
    File path: Absolute path of the photo.
    Camera: The name of the camera the photo was taken with.
    Time taken: The date and time of when the photo was taken.
    Latitude: Tthe latitude of the location where the photo was taken, if the photo is geotagged.
    Longitude: The longitude of the location where the photo was taken, if the photo is geotagged.
    Exposure time: Tthe exposure time of the photo.
    Aperture: The aperture the photo was taken with.
    ISO: The ISO setting the photo was taken with.
    Focal length: The focal length the photo was taken at.
"""

import argparse
import csv
import fractions
import os
import sqlite3

import exifread

COLUMNS = (
    ('File path', 'text'),
    ('Camera', 'text'),
    ('Time taken', 'text'),
    ('Latitude', 'real'),
    ('Longitude', 'real'),
    ('Exposure time', 'text'),
    ('Aperture', 'real'),
    ('ISO', 'integer'),
    ('Focal length', 'real')
)

def get_photo_metadata(photo_path):
    """Get the metadata of a single photo.

    Arguments:
        photo_path: String, absolute path of a photo.

    Returns dictionary with the following keys and values:
        File path: String, absolute path of the photo.
        Camera: String, name of the camera the photo was taken with.
        Time taken: String with the date and time of when the photo was taken, in the format: YYYY:MM:DD HH:MM:SS
        Latitude: Float, the latitude of the location where the photo was taken, if the photo is geotagged. Otherwise
            None.
        Longitude: Float, the longitude of the location where the photo was taken, if the photo is geotagged. Otherwise
            None.
        Exposure time: String, the exposure time of the photo represented as a fraction (Eg, "1/400") if under one
            second (Or not a whole second), otherwise the number of seconds (Eg, "6").
        Aperture: Float, the aperture the photo was taken with.
        ISO: The ISO setting the photo was taken with.
        Focal length: Float, the focal length the photo was taken at.
    """

    with open(photo_path, 'rb') as f:
        exif = exifread.process_file(f, details=False)

        if exif.get('GPS GPSLatitude') is not None and exif.get('GPS GPSLongitude') is not None \
                and exif.get('GPS GPSLatitudeRef') is not None and exif.get('GPS GPSLongitudeRef') is not None:
            lat_degrees, lat_minutes, lat_seconds = \
                [val.num / float(val.den) for val in exif['GPS GPSLatitude'].values]
            lon_degrees, lon_minutes, lon_seconds = \
                [val.num / float(val.den) for val in exif['GPS GPSLongitude'].values]

            latitude = lat_degrees + (lat_minutes/60) + (lat_seconds/3600)
            longitude = lon_degrees + (lon_minutes/60) + (lon_seconds/3600)

            if str(exif['GPS GPSLatitudeRef']).lower() == 's':
                latitude *= -1

            if str(exif['GPS GPSLongitudeRef']).lower() == 'w':
                longitude *= -1
        else:
            latitude = None
            longitude = None

    aperture = exif.get('EXIF FNumber')
    focal_length = exif.get('EXIF FocalLength')

    if aperture is not None:
        aperture = float(fractions.Fraction(str(aperture.values[0])))
    if focal_length is not None:
        focal_length = float(fractions.Fraction(str(focal_length.values[0])))

    return {
        'File path': photo_path,
        'Camera': exif.get('Image Model'),
        'Time taken': exif.get('Image DateTime'),
        'Latitude': latitude,
        'Longitude': longitude,
        'Exposure time': exif.get('EXIF ExposureTime'),
        'Aperture': aperture,
        'ISO': exif.get('EXIF ISOSpeedRatings'),
        'Focal length': focal_length
    }

def get_photo_paths(root_directory, file_extensions):
    """Generator to get the absolute paths of all photos with an extension matching a provided list of extensions.

    Arguments:
        root_directory: String, absolute path of the root directory to search for photos.
        file_extensions: List of case-insensitive strings containing the file extensions of photos to search for,
            without a preceeding period.

    Yields strings, the absolute path of a photo.
    """

    for root, _, files in os.walk(root_directory):
        for name in files:
            print(os.path.join(root, name))
            if os.path.splitext(name)[1].strip('.').lower() in file_extensions:
                yield os.path.join(root, name)

def write_csv_file(root_directory, file_extensions, output_file_path):
    """Write the metadata of all photos in a directory tree to a CSV file.

    Arguments:
        root_directory: String, absolute path of the root directory to search for photos.
        file_extensions: List of case-insensitive strings containing the file extensions of photos to search for,
            without a preceeding period.
        output_file_path: String, absolute path of the destination of the output CSV file. If None, a default path will
            be used.
    """

    if output_file_path is None:
        output_file_path = 'photos.csv'
    csv_file = open(output_file_path, 'wb')
    writer = csv.DictWriter(csv_file, fieldnames=[col[0] for col in COLUMNS])
    writer.writeheader()

    for photo in get_photo_paths(root_directory, file_extensions):
        writer.writerow(get_photo_metadata(photo))

    csv_file.close()

def write_database(root_directory, file_extensions, output_file_path):
    """Write the metadata of all photos in a directory tree to a SQLite database.

    Arguments:
        root_directory: String, absolute path of the root directory to search for photos.
        file_extensions: List of case-insensitive strings containing the file extensions of photos to search for,
            without a preceeding period.
        output_file_path: String, absolute path of the destination of the output SQLite database file. If None, a
            default path will be used.
    """

    if output_file_path is None:
        output_file_path = 'photos.db'
    if os.path.exists(output_file_path):
        os.remove(output_file_path)
    connection = sqlite3.connect(output_file_path)
    cursor = connection.cursor()
    cursor.execute('CREATE TABLE photos (%s)' % (','.join(['"%s" %s' % col for col in COLUMNS])))
    connection.commit()

    for photo in get_photo_paths(root_directory, file_extensions):
        photo_metadata = get_photo_metadata(photo)
        columns = ','.join(['`%s`' % key for key in photo_metadata.keys()])
        values = ','.join(['"%s"' % value for value in photo_metadata.values()])
        cursor.execute('INSERT INTO photos (%s) VALUES (%s)' % (columns, values))

    connection.commit()
    connection.close()

def main(root_directory, file_extensions, output_format, output_file):
    if output_format == 'csv':
        write_csv_file(root_directory, file_extensions, output_file)
    elif output_format == 'sqlite':
        write_database(root_directory, file_extensions, output_file)
    else:
        raise ValueError('Invalid output format')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('path',
                        help='Absolute path of the directory to recursively search for photos.')
    parser.add_argument('-f', '--format',
                        choices=['csv', 'sqlite'],
                        required=True,
                        dest='output_format',
                        help='Output format, either a CSV file or SQLite database.')
    parser.add_argument('-o', '--output_file', '--output-file',
                        nargs='+',
                        default=None,
                        dest='output_file',
                        help='Relative path of the output file. Default value is either "photos.csv" or "photos.db", '\
                             'depending on the output format.')
    parser.add_argument('-e', '--extensions',
                        nargs='+',
                        default=['jpg'],
                        dest='extensions',
                        help='Case-insensitive file extensions of photo files to get the metadata of.')
    args = parser.parse_args()

    main(root_directory=args.path,
         file_extensions=args.extensions,
         output_format=args.output_format,
         output_file=args.output_file)
