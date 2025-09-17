import boto3
import re
from botocore.exceptions import ClientError, NoCredentialsError
from urllib.parse import urlparse
import concurrent.futures
import time

class S3AccessChecker:
    def __init__(self, region_name='us-east-1'):
        """Initialize the S3 client"""
        try:
            self.s3_client = boto3.client('s3', region_name=region_name)
        except NoCredentialsError:
            raise Exception("AWS credentials not found. Please configure your credentials.")
    
    def parse_s3_url(self, s3_url):
        """Parse S3 URL to extract bucket and key"""
        if not s3_url.startswith('s3://'):
            raise ValueError(f"Invalid S3 URL format: {s3_url}")
        
        # Remove s3:// prefix and split
        path = s3_url[5:]  # Remove 's3://'
        parts = path.split('/', 1)
        
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ''
        
        return bucket, key
    
    def check_bucket_access(self, bucket_name):
        """Check if we have any access to the bucket"""
        try:
            # Try to get bucket location (minimal permission required)
            self.s3_client.get_bucket_location(Bucket=bucket_name)
            return True, "Bucket accessible"
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                return False, "Bucket does not exist"
            elif error_code == 'AccessDenied':
                return False, "Access denied to bucket"
            elif error_code == 'Forbidden':
                return False, "Forbidden - no permissions"
            else:
                return False, f"Error: {error_code}"
    
    def check_object_access(self, bucket_name, object_key):
        """Check if we have access to a specific object"""
        try:
            # Try to get object metadata (head_object is less expensive than get_object)
            self.s3_client.head_object(Bucket=bucket_name, Key=object_key)
            return True, "Object accessible"
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                return False, "Object does not exist"
            elif error_code == 'NoSuchBucket':
                return False, "Bucket does not exist"
            elif error_code in ['AccessDenied', 'Forbidden']:
                return False, "Access denied to object"
            else:
                return False, f"Error: {error_code}"
    
    def check_s3_url_access(self, s3_url):
        """Check access to an S3 URL (bucket or object)"""
        try:
            bucket, key = self.parse_s3_url(s3_url)
            
            # If no key specified, check bucket access
            if not key:
                accessible, message = self.check_bucket_access(bucket)
                return {
                    'url': s3_url,
                    'bucket': bucket,
                    'key': key,
                    'accessible': accessible,
                    'message': message,
                    'type': 'bucket'
                }
            else:
                # Check object access first
                accessible, message = self.check_object_access(bucket, key)
                return {
                    'url': s3_url,
                    'bucket': bucket,
                    'key': key,
                    'accessible': accessible,
                    'message': message,
                    'type': 'object'
                }
        except ValueError as e:
            return {
                'url': s3_url,
                'bucket': '',
                'key': '',
                'accessible': False,
                'message': str(e),
                'type': 'invalid'
            }
        except Exception as e:
            return {
                'url': s3_url,
                'bucket': '',
                'key': '',
                'accessible': False,
                'message': f"Unexpected error: {str(e)}",
                'type': 'error'
            }
    
    def check_multiple_urls(self, s3_urls, max_workers=5):
        """Check access to multiple S3 URLs concurrently"""
        results = []
        
        # Use ThreadPoolExecutor for concurrent checks
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_url = {
                executor.submit(self.check_s3_url_access, url): url 
                for url in s3_urls
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_url):
                result = future.result()
                results.append(result)
        
        return results
    
    def print_results_with_domains(self, results):
        """Print results showing both original domain and converted S3 URL"""
        print(f"\n{'='*100}")
        print(f"S3 Access Check Results ({len(results)} domains)")
        print(f"{'='*100}")
        
        accessible_count = 0
        for result in results:
            status = "✓ ACCESSIBLE" if result['accessible'] else "✗ NOT ACCESSIBLE"
            accessible_count += result['accessible']
            
            print(f"\nOriginal: {result['original_domain']}")
            print(f"S3 URL: {result['url']}")
            print(f"Status: {status}")
            print(f"Type: {result['type']}")
            print(f"Message: {result['message']}")
            if result['bucket']:
                print(f"Bucket: {result['bucket']}")
            if result['key']:
                print(f"Key: {result['key']}")
        
        print(f"\n{'='*100}")
        print(f"Summary: {accessible_count}/{len(results)} domains accessible")
        print(f"{'='*100}")
        
        return results
    
    def print_results(self, results):
        """Print results in a formatted way"""
        print(f"\n{'='*80}")
        print(f"S3 Access Check Results ({len(results)} URLs)")
        print(f"{'='*80}")
        
        accessible_count = 0
        for result in results:
            status = "✓ ACCESSIBLE" if result['accessible'] else "✗ NOT ACCESSIBLE"
            accessible_count += result['accessible']
            
            print(f"\nURL: {result['url']}")
            print(f"Status: {status}")
            print(f"Type: {result['type']}")
            print(f"Message: {result['message']}")
            if result['bucket']:
                print(f"Bucket: {result['bucket']}")
            if result['key']:
                print(f"Key: {result['key']}")
        
        print(f"\n{'='*80}")
        print(f"Summary: {accessible_count}/{len(results)} URLs accessible")
        print(f"{'='*80}")
        
        return results

def convert_domain_to_s3_url(domain):
    """Convert various S3 domain formats to s3:// URL format"""
    domain = domain.strip()
    
    # Remove protocol if present
    if domain.startswith('https://'):
        domain = domain[8:]
    elif domain.startswith('http://'):
        domain = domain[7:]
    
    # Handle different S3 domain formats
    if '.s3.' in domain or '.s3-' in domain:
        # Format: bucket-name.s3.region.amazonaws.com/path
        # or: bucket-name.s3-region.amazonaws.com/path
        parts = domain.split('/', 1)
        host = parts[0]
        path = parts[1] if len(parts) > 1 else ''
        
        # Extract bucket name (everything before .s3)
        if '.s3.' in host:
            bucket = host.split('.s3.')[0]
        elif '.s3-' in host:
            bucket = host.split('.s3-')[0]
        else:
            bucket = host
        
        # Construct s3:// URL
        if path:
            return f"s3://{bucket}/{path}"
        else:
            return f"s3://{bucket}"
    
    elif domain.startswith('s3.') or domain.startswith('s3-'):
        # Format: s3.region.amazonaws.com/bucket-name/path
        # or: s3-region.amazonaws.com/bucket-name/path
        parts = domain.split('/', 2)
        if len(parts) >= 2:
            bucket = parts[1]
            path = parts[2] if len(parts) > 2 else ''
            
            if path:
                return f"s3://{bucket}/{path}"
            else:
                return f"s3://{bucket}"
    
    elif 's3.amazonaws.com' in domain:
        # Format: s3.amazonaws.com/bucket-name/path
        parts = domain.split('/', 2)
        if len(parts) >= 2:
            bucket = parts[1]
            path = parts[2] if len(parts) > 2 else ''
            
            if path:
                return f"s3://{bucket}/{path}"
            else:
                return f"s3://{bucket}"
    
    elif domain.startswith('s3://'):
        # Already in correct format
        return domain
    
    else:
        # Assume it's just a bucket name
        if '/' in domain:
            parts = domain.split('/', 1)
            bucket = parts[0]
            path = parts[1]
            return f"s3://{bucket}/{path}"
        else:
            return f"s3://{domain}"

def load_urls_from_file(file_path):
    """Load S3 domains/URLs from a text file and convert to s3:// format"""
    urls = []
    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                original = line.strip()
                if original and not original.startswith('#'):  # Skip empty lines and comments
                    try:
                        s3_url = convert_domain_to_s3_url(original)
                        urls.append({
                            'original': original,
                            's3_url': s3_url,
                            'line_num': line_num
                        })
                    except Exception as e:
                        print(f"Warning: Could not convert line {line_num}: '{original}' - {e}")
        return urls
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")
    except Exception as e:
        raise Exception(f"Error reading file {file_path}: {e}")

# Example usage
def main():
    import argparse
    
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(
        description='Check access to S3 URLs from a text file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python s3_checker.py urls.txt
  python s3_checker.py my_s3_urls.txt --workers 10
  python s3_checker.py s3_list.txt --region us-west-2

File format (one domain per line):
  bucket-name.s3.amazonaws.com
  my-bucket.s3.us-west-2.amazonaws.com/path/file.txt
  s3.amazonaws.com/bucket-name
  https://bucket.s3-us-east-1.amazonaws.com/file.pdf
  just-a-bucket-name
  # This is a comment and will be ignored
        """
    )
    
    parser.add_argument(
        'file_path',
        help='Path to text file containing S3 URLs (one per line)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=5,
        help='Number of concurrent workers (default: 5)'
    )
    
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    
    parser.add_argument(
        '--output',
        help='Output results to CSV file (optional)'
    )
    
    args = parser.parse_args()
    
    try:
        # Load domains from file and convert to S3 URLs
        print(f"Loading S3 domains from: {args.file_path}")
        url_data = load_urls_from_file(args.file_path)
        
        if not url_data:
            print("No valid domains found in file.")
            return
        
        print(f"Found {len(url_data)} domains to check")
        
        # Show conversions
        print("\nDomain conversions:")
        for data in url_data[:10]:  # Show first 10 conversions
            print(f"  {data['original']} -> {data['s3_url']}")
        if len(url_data) > 10:
            print(f"  ... and {len(url_data) - 10} more")
        
        # Extract just the S3 URLs for checking
        s3_urls = [data['s3_url'] for data in url_data]
        
        # Initialize the checker
        checker = S3AccessChecker(region_name=args.region)
        
        # Check access to all URLs
        print(f"Checking S3 URL access using {args.workers} workers...")
        results = checker.check_multiple_urls(s3_urls, max_workers=args.workers)
        
        # Print formatted results with original domains
        results_with_original = []
        for i, result in enumerate(results):
            result['original_domain'] = url_data[i]['original']
            results_with_original.append(result)
        
        checker.print_results_with_domains(results_with_original)
        
        # Save to CSV if requested
        if args.output:
            save_results_to_csv(results_with_original, args.output)
            print(f"\nResults saved to: {args.output}")
        
        # Show accessible URLs
        accessible_results = [r for r in results_with_original if r['accessible']]
        if accessible_results:
            print(f"\nAccessible domains ({len(accessible_results)}):")
            for result in accessible_results:
                print(f"  - {result['original_domain']} ({result['url']})")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1

def save_results_to_csv(results, output_file):
    """Save results to a CSV file"""
    import csv
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['original_domain', 'url', 'accessible', 'type', 'message', 'bucket', 'key']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        for result in results:
            writer.writerow({
                'original_domain': result.get('original_domain', ''),
                'url': result['url'],
                'accessible': result['accessible'],
                'type': result['type'],
                'message': result['message'],
                'bucket': result['bucket'],
                'key': result['key']
            })

if __name__ == "__main__":
    import sys
    sys.exit(main() or 0)