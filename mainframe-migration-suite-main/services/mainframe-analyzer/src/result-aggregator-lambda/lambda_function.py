import json
import boto3
import os
import time
import traceback
from typing import Dict, Any, List

# Initialize clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def update_job_status(job_id: str, status: str, message: str = None) -> None:
    """Updates the job status in DynamoDB."""
    table_name = os.environ.get('JOBS_TABLE_NAME', 'MainframeAnalyzerJobs')
    
    try:
        table = dynamodb.Table(table_name)
        update_expression = 'SET #status = :status, updated_at = :time'
        expression_attr_names = {'#status': 'status'}
        expression_attr_values = {
            ':status': status,
            ':time': int(time.time())
        }
        
        if message:
            update_expression += ', status_message = :message'
            expression_attr_values[':message'] = message
        
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attr_names,
            ExpressionAttributeValues=expression_attr_values
        )
    except Exception as e:
        print(f"Error updating job status: {str(e)}")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Collate chunk summaries and perform cross-chunk analysis
    """
    print("=== RESULT AGGREGATOR LAMBDA HANDLER STARTED ===")
    
    try:
        # Extract parameters from the event
        job_id = event.get('job_id')
        bucket_name = event.get('bucket_name')
        output_path = event.get('output_path')
        chunk_results = event.get('chunk_results', [])
        
        print(f"[JOB] ID: {job_id}, Bucket: {bucket_name}, Path: {output_path}")
        print(f"[AGGREGATION] Processing {len(chunk_results)} chunk summaries")
        
        # Validate required parameters
        if not all([job_id, bucket_name, output_path]):
            error_message = "Missing required parameters"
            update_job_status(job_id, 'ERROR', error_message)
            return {'status': 'error', 'error': error_message}
        
        # Update job status
        update_job_status(job_id, 'AGGREGATING', f"Collating summaries from {len(chunk_results)} chunks and performing cross-chunk analysis")
        
        # Collect all chunk summaries
        all_summaries = []
        
        for result in chunk_results:
            if result.get('status') == 'error':
                print(f"[AGGREGATION] Skipping failed chunk {result.get('chunk_index')}")
                continue
                
            # Get the summary from S3 using summary_key from new ChunkProcessor format
            summary_key = result.get('summary_key')
            if not summary_key:
                print(f"[AGGREGATION] Missing summary_key in chunk result: {result}")
                continue
                
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=summary_key)
                summary_content = response['Body'].read().decode('utf-8')
                
                chunk_index = result.get('chunk_index')
                all_summaries.append(f"## CHUNK {chunk_index} ANALYSIS\n\n{summary_content}")
                print(f"[AGGREGATION] Collected summary from chunk {chunk_index} ({len(summary_content)} chars)")
                
            except Exception as e:
                print(f"[ERROR] Failed to read summary {summary_key}: {str(e)}")
                continue
        
        # Combine all summaries
        combined_summaries = "\n\n" + "="*80 + "\n\n".join(all_summaries)
        
        # Create comprehensive analysis prompt with cross-chunk analysis
        analysis_prompt = f"""Please analyze the following mainframe documentation summaries and provide comprehensive modernization recommendations with structured AWS implementations.

**CROSS-CHUNK ANALYSIS REQUIRED:**
Perform cross-chunk analysis to identify:
- Common patterns and shared components across chunks
- Integration points between different system components
- Consolidated data models and shared services
- Unified security and compliance requirements
- End-to-end workflow orchestration needs

**CHUNK SUMMARIES TO ANALYZE:**
{combined_summaries}

**REQUIRED OUTPUT:**
Provide a complete modernization solution organized by AWS service categories. Create specific implementation files for each service category with cross-chunk integration considerations.

Focus on:
- Production-ready, secure implementations
- Cross-chunk component integration
- Unified data architecture
- End-to-end workflow orchestration
- Consolidated security model
- Cost optimization strategies
- Migration-specific considerations for mainframe workloads

Provide complete, deployable code for each component with proper cross-system integration.
"""
        
        # Save the aggregated analysis prompt for Analysis Lambda
        full_prompt_key = f"{output_path}/{job_id}/aggregated_analysis_prompt.txt"
        print(f"[S3] Saving aggregated analysis prompt to: {full_prompt_key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=full_prompt_key,
            Body=analysis_prompt.encode('utf-8'),
            ContentType='text/plain'
        )
        
        # Update job status
        status_message = f"Aggregated {len(chunk_results)} chunk summaries and prepared cross-chunk analysis prompt"
        update_job_status(job_id, 'AGGREGATED', status_message)
        
        print(f"[SUCCESS] Aggregation completed: {len(chunk_results)} chunks processed")
        
        return {
            'job_id': job_id,
            'bucket_name': bucket_name,
            'full_prompt_key': full_prompt_key,  # For Analysis Lambda
            'output_path': output_path,
            'status': 'success',
            'chunks_processed': len(chunk_results),
            'cross_chunk_analysis': True,
            'message': status_message
        }
        
    except Exception as e:
        error_message = f"Result aggregation failed: {str(e)}"
        print(f"[ERROR] {error_message}")
        print(traceback.format_exc())
        
        if 'job_id' in event:
            update_job_status(event['job_id'], 'FAILED', error_message)
        
        return {
            'job_id': job_id if 'job_id' in locals() else None,
            'bucket_name': bucket_name if 'bucket_name' in locals() else None,
            'output_path': output_path if 'output_path' in locals() else None,
            'status': 'FAILED',
            'error': error_message
        }
