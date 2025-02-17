import os
import logging
from datetime import datetime, timezone
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

    def list_objects(
        self, 
        prefix: str = "",
        max_items: Optional[int] = None,
        file_types: Optional[Union[List[str], str]] = None
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
            if file_types is not None:
                if isinstance(file_types, str):
                    file_types = [file_types]
                # Remove leading dots and convert to lowercase
                file_types_lower = [ext.lstrip('.').lower() for ext in file_types]

            paginator = self.s3_client.get_paginator('list_objects_v2')
            config = {'MaxItems': max_items} if max_items else {}
            
            results = []
            for page in paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix,
                PaginationConfig=config
            ):
                results.extend([
                    {
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].astimezone(timezone.utc),
                        'etag': obj['ETag'],
                        'storage_class': obj.get('StorageClass', 'STANDARD')
                    }
                    for obj in page.get('Contents', [])
                    # Apply file type filter if specified
                    if (file_types_lower is None) or (
                        '.' in obj['Key'] and 
                        obj['Key'].rsplit('.', 1)[1].lstrip('.').lower() in file_types_lower
                    )
                ])
            
            logger.info("Listed %d objects from prefix: %s%s", 
                       len(results), 
                       prefix, 
                       f", file types: {file_types}" if file_types else "")
            return results
            
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
        Lista todas las carpetas y subcarpetas bajo un prefijo específico en S3.

        :param prefix: Prefijo S3 desde donde empezar a listar (por defecto: raíz)
        :return: Lista ordenada de rutas de carpetas
        :raises S3OperationError: Si falla la operación
        """
        try:
            # Normalizar el prefijo para asegurar que termine en '/' si no está vacío
            normalized_prefix = prefix
            if normalized_prefix and not normalized_prefix.endswith('/'):
                normalized_prefix += '/'
            
            folders = set()
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            # Listar todos los objetos bajo el prefijo
            for page in paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=normalized_prefix
            ):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    # Generar todas las carpetas padre para la clave
                    parts = key.split('/')
                    current_folder = ''
                    for part in parts[:-1]:  # Excluir el último elemento (archivo o cadena vacía)
                        if part:
                            current_folder += part + '/'
                        # Verificar si la carpeta está bajo el prefijo normalizado
                        if current_folder.startswith(normalized_prefix):
                            folders.add(current_folder)
            
            # Remover el prefijo normalizado si está presente
            if normalized_prefix:
                folders.discard(normalized_prefix)
            
            # Ordenar y retornar
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