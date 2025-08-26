"""
IBM Cloud Pak for Data (CPD) client for API connections
"""

import requests
import urllib3
import os
import threading
import time
import queue
import logging
from typing import Dict, Optional, Any
from dataclasses import dataclass
from contextlib import contextmanager
from dotenv import load_dotenv

urllib3.disable_warnings()

@dataclass
class TokenInfo:
    """Token information for CPD authentication"""
    token: str
    created_at: float  # Unix timestamp when token was created
    lock: threading.RLock
    last_auth_failure: Optional[float] = None  # Track when auth last failed
    
    def should_refresh_on_time(self, max_age_hours: float = 23.0) -> bool:
        """Check if token should be refreshed based on age"""
        with self.lock:
            age_hours = (time.time() - self.created_at) / 3600
            return age_hours >= max_age_hours
    
    def mark_auth_failure(self):
        """Mark that this token caused an authentication failure"""
        with self.lock:
            self.last_auth_failure = time.time()
    
    def should_refresh_on_failure(self, min_retry_interval: float = 60.0) -> bool:
        """Check if enough time has passed since last auth failure to retry"""
        with self.lock:
            if self.last_auth_failure is None:
                return True
            return time.time() - self.last_auth_failure >= min_retry_interval


class CPDClient:
    """
    CPD client that manages multiple sessions and handles authentication centrally.
    Designed for CPD on-premises installations.
    """
    
    def __init__(self, config_file=None, pool_size: int = 50, max_token_age_hours: float = 23.0):
        """
        Initialize CPD client
        
        Args:
            config_file: Optional path to .env file
            pool_size: Maximum number of sessions in the pool
            max_token_age_hours: Refresh token after this many hours
        """
        if config_file:
            load_dotenv(config_file, override=True)
        else:
            load_dotenv('.env', override=True)
            
        # Load configuration
        self.cpd_host = os.environ.get('CPD_HOST')
        self.username = os.environ.get('USERNAME')
        self.password = os.environ.get('PASSWORD')
        self.api_key = os.environ.get('API_KEY')
        self.auth_type = os.environ.get('AUTH_TYPE', 'PASSWORD')
        
        self._validate_config()
        
        # Initialize logger first
        self._logger = logging.getLogger(__name__)
        
        # Session pool
        self._session_pool = queue.Queue(maxsize=pool_size)
        self._pool_size = pool_size
        self._pool_lock = threading.Lock()
        self._sessions_created = 0
        
        # Token management
        self._token_info: Optional[TokenInfo] = None
        self._max_token_age_hours = max_token_age_hours
        self._auth_lock = threading.RLock()
        
        # Initialize pool with some sessions
        self._initialize_pool(min(10, pool_size))
        
        # Get initial token
        self._refresh_token()
    
    def _validate_config(self):
        """Validate required environment variables"""
        if not self.cpd_host:
            raise ValueError("CPD_HOST environment variable is required")
        
        if self.auth_type == "PASSWORD" and not (self.username and self.password):
            raise ValueError("USERNAME and PASSWORD required for password authentication")
        elif self.auth_type != "PASSWORD" and not (self.username and self.api_key):
            raise ValueError("USERNAME and API_KEY required for API key authentication")
    
    def _initialize_pool(self, initial_size: int):
        """Initialize the session pool with a number of sessions"""
        for _ in range(initial_size):
            session = self._create_session()
            self._session_pool.put(session)
            self._sessions_created += 1
    
    def _create_session(self) -> requests.Session:
        """Create a new configured session"""
        session = requests.Session()
        
        # Configure session for better performance and reliability
        session.mount('https://', requests.adapters.HTTPAdapter(
            max_retries=3,
            pool_connections=10,
            pool_maxsize=10
        ))
        
        # Set reasonable timeouts
        session.timeout = (10, 60)  # (connect, read) timeout
        
        return session
    
    def _refresh_token(self):
        """Refresh authentication token"""
        # Use authentication lock to prevent multiple simultaneous refresh attempts
        with self._auth_lock:
            # Double-check pattern: if another thread just refreshed, don't refresh again
            if (self._token_info is not None and 
                not self._token_info.should_refresh_on_time(self._max_token_age_hours)):
                return
            
            # Use a temporary session for authentication to avoid pool issues
            with requests.Session() as auth_session:
                auth_session.verify = False
                
                url = f"https://{self.cpd_host}/icp4d-api/v1/authorize"
                payload = (
                    {"username": self.username, "password": self.password} 
                    if self.auth_type == "PASSWORD" 
                    else {"username": self.username, "api_key": self.api_key}
                )
                
                try:
                    response = auth_session.post(url, json=payload, timeout=30, verify=False)
                except requests.RequestException as e:
                    raise ConnectionError(f"Error authenticating to CPD: {str(e)}")

                if response.status_code == 200:
                    token_data = response.json()
                    access_token = token_data['token']
                    
                    self._logger.info("CPD token refreshed successfully")
                    
                    # Update token info
                    if self._token_info is None:
                        self._token_info = TokenInfo(
                            token=access_token,
                            created_at=time.time(),
                            lock=threading.RLock()
                        )
                    else:
                        with self._token_info.lock:
                            self._token_info.token = access_token
                            self._token_info.created_at = time.time()
                            self._token_info.last_auth_failure = None  # Reset failure tracking
                    
                else:
                    raise ConnectionError(
                        f"CPD authentication failed.\n"
                        f"Status: {response.status_code}\n"
                        f"Response: {response.text}"
                    )
    
    def _get_current_token(self) -> str:
        """Get current valid token, refreshing if necessary based on age"""
        if self._token_info is None:
            self._refresh_token()
        
        # Check if we should refresh based on token age
        if self._token_info.should_refresh_on_time(self._max_token_age_hours):
            self._logger.info("Token refresh triggered (age-based)")
            self._refresh_token()
        
        with self._token_info.lock:
            return self._token_info.token
    
    def _handle_auth_failure(self):
        """Handle authentication failure by refreshing token if appropriate"""
        if self._token_info is None:
            self._refresh_token()
            return
        
        # Mark the failure and check if we should retry
        self._token_info.mark_auth_failure()
        
        if self._token_info.should_refresh_on_failure():
            self._logger.info("Token refresh triggered (authentication failure)")
            self._refresh_token()
        else:
            # Recent refresh attempt failed, don't retry immediately
            raise ConnectionError("Recent authentication refresh failed, waiting before retry")
    
    @contextmanager
    def get_session(self):
        """Get a session from the pool (context manager)"""
        session = None
        try:
            # Try to get an existing session
            try:
                session = self._session_pool.get_nowait()
            except queue.Empty:
                # Create new session if pool is empty and we haven't hit the limit
                with self._pool_lock:
                    if self._sessions_created < self._pool_size:
                        session = self._create_session()
                        self._sessions_created += 1
                    else:
                        # Wait for a session to become available
                        session = self._session_pool.get(timeout=30)
            
            # Configure session with current token and headers
            token = self._get_current_token()
            session.headers.update({
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {token}'
            })
            
            yield session
            
        finally:
            if session:
                # Clean up session-specific headers to avoid token leakage
                session.headers.pop('Authorization', None)
                # Return session to pool
                try:
                    self._session_pool.put_nowait(session)
                except queue.Full:
                    # Pool is full, close this session
                    session.close()
    
    def request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make an authenticated request using a pooled session"""
        url = f"https://{self.cpd_host}{endpoint}"
        
        # Try the request with current token
        with self.get_session() as session:
            response = session.request(method, url, verify=False, **kwargs)
            
            # If we get an authentication error, try to refresh token and retry once
            if response.status_code == 401:
                self._logger.warning(f"Authentication failure ({response.status_code}) for {method} {endpoint}, attempting token refresh")
                
                try:
                    self._handle_auth_failure()
                    
                    # Retry with new token
                    token = self._get_current_token()
                    session.headers.update({'Authorization': f'Bearer {token}'})
                    response = session.request(method, url, verify=False, **kwargs)
                    
                    if response.status_code == 401:
                        self._logger.error(f"Authentication still failing after token refresh for {method} {endpoint}")
                    else:
                        self._logger.info(f"Request succeeded after token refresh for {method} {endpoint}")
                        
                except ConnectionError as e:
                    self._logger.error(f"Token refresh failed: {e}")
                    # Return the original auth failure response
            
            return response
    
    def get(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated GET request"""
        return self.request('GET', endpoint, **kwargs)
    
    def post(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated POST request"""
        return self.request('POST', endpoint, **kwargs)
    
    def put(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated PUT request"""
        return self.request('PUT', endpoint, **kwargs)
    
    def patch(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated PATCH request"""
        return self.request('PATCH', endpoint, **kwargs)
    
    def delete(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated DELETE request"""
        return self.request('DELETE', endpoint, **kwargs)
    
    def search(self, query_payload: Dict[str, Any], auth_scope: str = "category") -> requests.Response:
        """Perform search using CPD search API"""
        endpoint = f"/v3/search?auth_scope={auth_scope}"
        return self.post(endpoint, json=query_payload)
    
    def close(self):
        """Close all sessions in the pool"""
        while True:
            try:
                session = self._session_pool.get_nowait()
                session.close()
            except queue.Empty:
                break
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()