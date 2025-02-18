import os
import logging
from datetime import datetime, timezone
import time
from typing import Optional, Dict, List, BinaryIO, Any, Union
import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from boto3.s3.transfer import TransferConfig

# Configurar logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

class S3ManagerError(Exception):
    """Base exception class for S3 Manager errors"""
    pass

class S3ConfigurationError(S3ManagerError):
    """Error in S3 configuration"""
    pass

class S3OperationError(S3ManagerError):
    """Error during S3 operation"""
    pass

class S3Manager:
    DEFAULT_REGION = "us-east-1"
    SIGNED_URL_EXPIRATION = 1800 # 30 minutes
    MULTIPART_THRESHOLD = 50 * 1024 * 1024  # 50 MB
    MAX_CONCURRENCY = 10

    def __init__(self, endpoint_url: Optional[str] = None):
        """
        Initialize S3 client with automatic credentials detection.
        
        :param endpoint_url: Custom endpoint URL for S3-compatible services
        :raises S3ConfigurationError: If required configuration is missing
        """
        self.bucket_name = os.getenv('AWS_BUCKET_NAME')
        self.region = os.getenv('AWS_REGION', self.DEFAULT_REGION)
        
        if not self.bucket_name:
            raise S3ConfigurationError("AWS_BUCKET_NAME environment variable not set")
            
        self.endpoint_url = self._validate_endpoint(endpoint_url)
        self.s3_client = self._init_client()
        self.transfer_config = TransferConfig(
            multipart_threshold=self.MULTIPART_THRESHOLD,
            max_concurrency=self.MAX_CONCURRENCY
        )

    def _validate_endpoint(self, endpoint_url: Optional[str]) -> str:
        """Validate and construct proper endpoint URL"""
        if endpoint_url:
            return endpoint_url.rstrip('/')
        return f"https://s3.{self.region}.amazonaws.com"

    def _init_client(self) -> boto3.client:
        """Initialize and configure S3 client with best practices"""
        try:
            session = boto3.Session(
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=self.region
            )
            
            client = session.client(
                's3',
                endpoint_url=self.endpoint_url,
                config=boto3.session.Config(
                    signature_version='s3v4',
                    connect_timeout=30,
                    read_timeout=60,
                    retries={'max_attempts': 5, 'mode': 'standard'}
                )
            )
            
            # Validate bucket accessibility
            client.head_bucket(Bucket=self.bucket_name)
            return client
            
        except (BotoCoreError, NoCredentialsError, ClientError) as e:
            logger.error("S3 client initialization failed: %s", e, extra={
                'bucket': self.bucket_name,
                'region': self.region
            })
            raise S3ConfigurationError("Failed to initialize S3 client") from e

    def get_total_size(
            self,
            prefix: str = ""
        ) -> int:
        """
        Calculate the total size of all objects in the bucket or under a specific prefix.

        :param prefix: S3 prefix (folder path) to calculate the size. Defaults to the entire bucket.
        :return: Total size in bytes.
        :raises S3OperationError: If the operation fails.
        """
        try:
            total_size = 0
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix
            ):
                for obj in page.get('Contents', []):
                    total_size += obj['Size']
            
            human_size = self._bytes_to_human(total_size)
            logger.info(
                "Total size for prefix '%s': %s (%d bytes)",
                prefix if prefix else 'root',
                human_size,
                total_size
            )
            return total_size
            
        except ClientError as e:
            logger.error(
                "Error calculating total size: %s",
                e,
                exc_info=True,
                extra={
                    'bucket': self.bucket_name,
                    'prefix': prefix
                }
            )
            raise S3OperationError(f"Error calculating size: {e}") from e

    def list_objects(
        self, 
        prefix: str = "",
        max_items: Optional[int] = None,
        file_types: Optional[Union[List[str], str]] = None,
        exclude_directories: bool = True
    ) -> List[Dict[str, Any]]:
        """
        List objects in S3 bucket with pagination and filtering
        
        :param prefix: S3 key prefix to filter objects
        :param max_items: Maximum number of items to return
        :param file_types: Optional list of file extensions (or a single extension as a string) 
                           to filter by. Extensions are case-insensitive and can include or omit 
                           the leading dot (e.g., 'txt', '.csv').
        :return: List of objects with metadata
        :raises S3OperationError: If operation fails
        """
        try:
            # Process file_types to handle various input formats
            file_types_lower = None
            if file_types:
                file_types = [file_types] if isinstance(file_types, str) else file_types
                file_types_lower = {ext.lstrip('.').lower() for ext in file_types}

            paginator = self.s3_client.get_paginator('list_objects_v2')
            results = []
            remaining = max_items if max_items is not None else float('inf')
            

            page_iterator = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix,
                PaginationConfig={'MaxItems': max_items * 2} if max_items else {}
            )

            for page in page_iterator:
                for obj in page.get('Contents', []):
                    if remaining <= 0:
                        break
                        
                    # Apply filters
                    key = obj['Key']
                    if exclude_directories and key.endswith('/'):
                        continue
                        
                    if file_types_lower:
                        if '.' not in key:
                            continue
                        ext = key.rsplit('.', 1)[1].lstrip('.').lower()
                        if ext not in file_types_lower:
                            continue

                    # Add valid object
                    results.append({
                        'key': key,
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].astimezone(timezone.utc),
                        'etag': obj['ETag'],
                        'storage_class': obj.get('StorageClass', 'STANDARD')
                    })
                    remaining -= 1

                if remaining <= 0:
                    break

            final_results = results[:max_items] if max_items else results

            logger.info("Listed %d objects from prefix: %s%s", 
                       len(results), 
                       prefix, 
                       f", file types: {file_types}" if file_types else "")
            return final_results
            
        except ClientError as e:
            logger.error("List objects failed: %s", e, exc_info=True, extra={
                'bucket': self.bucket_name,
                'prefix': prefix,
                'file_types': file_types
            })
            raise S3OperationError(f"List operation failed: {e}") from e

    def list_latest_files(
        self, 
        prefix: str = "",
        max_files: int = 10,
        descending: bool = True,
        file_types: Optional[Union[List[str], str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Lists most recent files sorted by modification date with optional type filtering

        :param prefix: S3 prefix to filter files
        :param max_files: Maximum number of files to return
        :param descending: Descending order (newest first)
        :param file_types: Optional list/single file extension to filter 
                         (e.g., ['txt', 'csv'] or 'pdf'). Case-insensitive.
        :return: List of sorted file metadata
        :raises S3OperationError: If the operation fails
        """
        try:
            # Normalize file types
            file_types_lower = None
            if file_types is not None:
                if isinstance(file_types, str):
                    file_types = [file_types]
                file_types_lower = [ext.lstrip('.').lower() for ext in file_types]

            all_objects = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix
            ):
                if 'Contents' in page:
                    # Combined filtering (folders and types)
                    all_objects.extend([
                        obj for obj in page['Contents']
                        if not obj['Key'].endswith('/') # Exclude folders
                        and (file_types_lower is None or (
                            ('.' in obj['Key'] and 
                             obj['Key'].rsplit('.', 1)[1].lower() in file_types_lower)
                            or ('.' not in obj['Key'] and 
                                '' in file_types_lower)
                        ))
                    ])

            # Sort by modification date
            sorted_objects = sorted(
                all_objects,
                key=lambda x: x['LastModified'],
                reverse=descending
            )[:max_files]

            # Format results
            return [{
                'key': obj['Key'],
                'size': obj['Size'],
                'last_modified': obj['LastModified'].astimezone(timezone.utc),
                'etag': obj['ETag'],
                'storage_class': obj.get('StorageClass', 'STANDARD')
            } for obj in sorted_objects]

        except ClientError as e:
            logger.error("Latest files listing failed: %s", e, exc_info=True, extra={
                'bucket': self.bucket_name,
                'prefix': prefix,
                'file_types': file_types
            })
            raise S3OperationError(f"Latest files listing failed: {e}") from e

    def list_folders(
        self,
        prefix: str = ""
    ) -> List[str]:
        """
        List all folders and subfolders under a specific prefix in S3.

        :param prefix: S3 prefix to start listing from (default: root)
        :return: Sorted list of folder paths
        :raises S3OperationError: If the operation fails
        """
        try:
            # Normalize the prefix to ensure it ends with '/' if not empty
            normalized_prefix = prefix
            if normalized_prefix and not normalized_prefix.endswith('/'):
                normalized_prefix += '/'
            
            folders = set()
            paginator = self.s3_client.get_paginator('list_objects_v2')
            # List all objects under the prefix
            for page in paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=normalized_prefix
            ):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    # Generate all parent folders for the key
                    parts = key.split('/')
                    current_folder = ''
                    for part in parts[:-1]:  # Exclude the last element (file or empty string)
                        if part:
                            current_folder += part + '/'
                        # Check if the folder is under the normalized prefix
                        if current_folder.startswith(normalized_prefix):
                            folders.add(current_folder)
            
            # Remove the normalized prefix if present
            if normalized_prefix:
                folders.discard(normalized_prefix)
            
            # Sort and return
            sorted_folders = sorted(folders)
            logger.info("Listadas %d carpetas bajo el prefijo: %s", 
                    len(sorted_folders), 
                    normalized_prefix or 'raíz')
            return sorted_folders
            
        except ClientError as e:
            logger.error("Error listando carpetas: %s", e, exc_info=True, extra={
                'bucket': self.bucket_name,
                'prefix': prefix
            })
            raise S3OperationError(f"Error listando carpetas: {e}") from e

    def get_file_in_memory(
            self, 
            key: str,
            version_id: Optional[str] = None,
            decode: bool = False,
            encoding: str = 'utf-8'
        ) -> Union[bytes, str]:
        """
        Load a file from S3 directly into memory

        :param key: S3 key of the file
        :param version_id: Specific version of the object (optional)
        :param decode: Decode the content to string
        :param encoding: Encoding to decode (default: utf-8)
        :return: File content as bytes or string
        :raises S3OperationError: If the file does not exist or download fails
        """
        try:
            params = {
                'Bucket': self.bucket_name,
                'Key': key
            }
            if version_id:
                params['VersionId'] = version_id

            response = self.s3_client.get_object(**params)
            content = response['Body'].read()

            if decode:
                return content.decode(encoding)
                
            logger.info("File loaded into memory: %s (%d bytes)", 
                    key, len(content))
            return content

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchKey':
                logger.error("File not found: %s", key)
                raise S3OperationError(f"File does not exist: {key}") from e
            logger.error("Error loading file: %s", e, exc_info=True)
            raise S3OperationError(f"Error loading file: {e}") from e

    def upload_fileobj(
        self,
        file_obj: BinaryIO,
        s3_key: str,
        metadata: Optional[Dict[str, str]] = None,
        content_type: Optional[str] = None,
        public_read: bool = False
    ) -> Dict[str, Any]:
        """
        Upload file-like object to S3 with enhanced options
        
        :param file_obj: File-like object to upload
        :param s3_key: Full S3 key/path for the object
        :param metadata: Optional object metadata
        :param content_type: Content MIME type
        :param public_read: Enable public read access
        :return: Upload result metadata
        :raises S3OperationError: If upload fails
        """
        try:
            extra_args = self._build_upload_args(
                metadata, 
                content_type, 
                public_read
            )
            
            self.s3_client.upload_fileobj(
                Fileobj=file_obj,
                Bucket=self.bucket_name,
                Key=s3_key,
                ExtraArgs=extra_args,
                Config=self.transfer_config
            )
            
            # Get uploaded object metadata
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            logger.info("Successfully uploaded: %s (%d bytes)", 
                      s3_key, response['ContentLength'])
            
            return {
                'key': s3_key,
                'size': response['ContentLength'],
                'etag': response['ETag'],
                'metadata': response.get('Metadata', {}),
                'content_type': response['ContentType'],
                'version_id': response.get('VersionId')
            }
            
        except (ClientError, BotoCoreError) as e:
            logger.error("Upload failed for key: %s - %s", s3_key, e, exc_info=True)
            raise S3OperationError(f"Upload failed: {e}") from e

    def upload_directory(
        self,
        directory_path: str,
        s3_prefix: str,
        metadata: Optional[Dict[str, str]] = None,
        content_type: Optional[str] = None,
        public_read: bool = False
    ) -> Dict[str, Any]:
        """
        Upload a directory to S3 recursively
        
        :param directory_path: Path to the local directory to upload
        :param s3_prefix: S3 prefix (folder) where the directory contents will be uploaded
        :param metadata: Optional object metadata
        :param content_type: Content MIME type
        :param public_read: Enable public read access
        :return: Dictionary with upload results for each file
        :raises S3OperationError: If upload fails for any file
        """
        upload_results = {}
        
        for root, dirs, files in os.walk(directory_path):
            for file_name in files:
                local_file_path = os.path.join(root, file_name)
                
                # Construct the S3 key by joining the prefix and the relative path
                relative_path = os.path.relpath(local_file_path, directory_path)
                s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
                
                try:
                    with open(local_file_path, 'rb') as file_obj:
                        result = self.upload_fileobj(
                            file_obj=file_obj,
                            s3_key=s3_key,
                            metadata=metadata,
                            content_type=content_type,
                            public_read=public_read
                        )
                        upload_results[local_file_path] = result
                except S3OperationError as e:
                    logger.error("Failed to upload file: %s - %s", local_file_path, e)
                    upload_results[local_file_path] = {'error': str(e)}
        
        return upload_results

    def download(
        self,
        s3_target: str,
        local_path: str,
        overwrite: bool = False
    ) -> Dict[str, Union[int, float]]:
        """
        Download a file or entire directory from S3

        :param s3_target: S3 path (file or directory prefix)
        :param local_path: Local destination path
        :param overwrite: Overwrite existing files
        :return: Dict with download statistics
        :raises S3OperationError: If the operation fails
        """
        try:
            total_files = 0
            total_size = 0
            start_time = time.time()

            # Determine if it is a file or directory
            is_directory = s3_target.endswith('/') or not self._s3_object_exists(s3_target)

            if is_directory:
                if os.path.exists(local_path):
                    if not os.path.isdir(local_path):
                        raise ValueError(f"The local path exists and is not a directory: {local_path}")
                else:
                    os.makedirs(local_path, exist_ok=True)

                # Download entire directory
                objects = self.list_objects(prefix=s3_target)
                if not objects:
                    raise S3OperationError(f"Empty directory or does not exist: {s3_target}")

                for obj in objects:
                    relative_path = os.path.relpath(obj['key'], s3_target)
                    dest_path = os.path.join(local_path, relative_path)
                    print(dest_path)

                    if os.path.exists(dest_path) and os.path.isdir(dest_path):
                        raise IsADirectoryError(
                        f"Cannot overwrite directory with file: {dest_path}"
                    )

                    # self._download_single_file(obj['key'], dest_path, overwrite)
                    total_files += 1
                    total_size += obj['size']

            else:
                if os.path.isdir(local_path):
                    filename = os.path.basename(s3_target)
                    dest_path = os.path.join(local_path, filename)
                else:
                    dest_path = local_path

                # Download individual file
                self._download_single_file(s3_target, dest_path, overwrite)
                total_files = 1
                total_size = self._get_object_size(s3_target)

            elapsed = time.time() - start_time
            logger.info(
                "Download completed: %d files (%s) in %.1fs",
                total_files,
                self._bytes_to_human(total_size),
                elapsed
            )

            return {
                'total_files': total_files,
                'total_bytes': total_size,
                'time_sec': round(elapsed, 1),
                'avg_speed': total_size / elapsed if elapsed > 0 else 0
            }

        except ClientError as e:
            logger.error("Download error: %s", e, exc_info=True, extra={
                's3_target': s3_target,
                'local_path': local_path
            })
            raise S3OperationError(f"Download error: {e}") from e

    def _download_single_file(self, s3_key: str, local_path: str, overwrite: bool):
        """Download an individual file"""
        dest_dir = os.path.dirname(local_path)

        if os.path.exists(dest_dir) and not os.path.isdir(dest_dir):
            raise NotADirectoryError(
            f"Cannot create directory '{dest_dir}' because a file with that name already exists"
        )
        os.makedirs(dest_dir, exist_ok=True)

        if os.path.exists(local_path):
            if os.path.isdir(local_path):
                raise IsADirectoryError(f"The destination path is a directory: {local_path}")
            if not overwrite:
                raise FileExistsError(f"File already exists: {local_path}")

        self.s3_client.download_file(
            Bucket=self.bucket_name,
            Key=s3_key,
            Filename=local_path
        )
        logger.debug("Downloaded: %s -> %s", s3_key, local_path)

    def _s3_object_exists(self, key: str) -> bool:
        """Check if an object exists"""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

    def _get_object_size(self, key: str) -> int:
        """Get the size of an individual object"""
        response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
        return response['ContentLength']

    def _build_upload_args(
        self,
        metadata: Optional[Dict[str, str]],
        content_type: Optional[str],
        public_read: bool
    ) -> Dict[str, Any]:
        """Construct ExtraArgs dictionary for upload operations"""
        extra_args = {}
        
        if metadata:
            extra_args['Metadata'] = {
                k: str(v) for k, v in metadata.items()
            }
            
        if content_type:
            extra_args['ContentType'] = content_type
            
        if public_read:
            extra_args['ACL'] = 'public-read'
            
        return extra_args
    
    def generate_presigned_url(
        self,
        s3_key: str,
        expiration: int = SIGNED_URL_EXPIRATION,
        method: str = 'get_object'
    ) -> str:
        """
        Generate presigned URL for secure object access
        
        :param s3_key: S3 object key
        :param expiration: URL validity in seconds
        :param method: HTTP method (get_object/put_object)
        :return: Presigned URL
        :raises S3OperationError: If URL generation fails
        """
        try:
            return self.s3_client.generate_presigned_url(
                ClientMethod=method,
                Params={
                    'Bucket': self.bucket_name,
                    'Key': s3_key
                },
                ExpiresIn=expiration,
                HttpMethod='GET' if method == 'get_object' else 'PUT'
            )
        except ClientError as e:
            logger.error("Presigned URL generation failed: %s", e, exc_info=True)
            raise S3OperationError(f"URL generation failed: {e}") from e

    @staticmethod
    def generate_s3_key(
        original_name: str,
        prefix: Optional[str] = None,
        unique_id: Optional[str] = None
    ) -> str:
        """
        Generate unique S3 key with timestamp and optional unique ID
        
        :param original_name: Original file name
        :param prefix: Key prefix (folder path)
        :param unique_id: Additional unique identifier
        :return: Generated S3 key
        """
        base_name = os.path.basename(original_name)
        name_part, ext = os.path.splitext(base_name)
        
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')
        components = [prefix, name_part, timestamp]
        
        if unique_id:
            components.append(unique_id)
            
        clean_components = [c for c in components if c]
        return f"{'/'.join(clean_components)}{ext}".lstrip('/')

    @staticmethod
    def _bytes_to_human(size_bytes: int) -> str:
        """
        Convert bytes to human-readable format (KB, MB, GB, TB)
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"