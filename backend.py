import modal
import io
import zipfile
import struct
import json
from typing import Generator
import hashlib

# Create Modal app with required dependencies
app = modal.App("bin-extractor")

image = modal.Image.debian_slim().pip_install(
    "fastapi[standard]",
    "python-multipart"
)

# Temporary storage for uploaded files
volume = modal.Volume.from_name("bin-files", create_if_missing=True)

def extract_binary_file(file_content: bytes) -> list[dict]:
    """
    Extract files from a binary blob.
    This is a generic extractor that looks for common file signatures.
    """
    files = []
    
    # Common file signatures
    signatures = {
        b'\x89PNG\r\n\x1a\n': ('png', 8),
        b'GIF89a': ('gif', 6),
        b'GIF87a': ('gif', 6),
        b'\xff\xd8\xff': ('jpg', 3),
        b'PK\x03\x04': ('zip', 4),
        b'%PDF': ('pdf', 4),
        b'MZ': ('exe', 2),
        b'\x1f\x8b': ('gz', 2),
        b'BM': ('bmp', 2),
        b'RIFF': ('wav', 4),
    }
    
    # Search for file signatures in the binary data
    for i in range(len(file_content) - 10):
        for sig, (ext, sig_len) in signatures.items():
            if file_content[i:i+sig_len] == sig:
                # Found a file signature, try to extract
                file_id = hashlib.md5(file_content[i:i+100]).hexdigest()[:8]
                
                # Try to find the end of the file (next signature or end of data)
                end_pos = i + 1024  # Default chunk size
                for j in range(i + sig_len, min(i + 10485760, len(file_content))):  # Max 10MB per file
                    for next_sig, _ in signatures.items():
                        if file_content[j:j+len(next_sig)] == next_sig:
                            end_pos = j
                            break
                    if end_pos != i + 1024:
                        break
                
                if end_pos == i + 1024:
                    end_pos = min(i + 1024*1024, len(file_content))  # 1MB default
                
                file_data = file_content[i:end_pos]
                files.append({
                    'name': f'extracted_{file_id}.{ext}',
                    'size': len(file_data),
                    'offset': i,
                    'data': file_data,
                    'type': ext
                })
    
    # If no files found, treat the entire binary as a single file
    if not files:
        files.append({
            'name': 'binary_data.bin',
            'size': len(file_content),
            'offset': 0,
            'data': file_content,
            'type': 'bin'
        })
    
    return files


@app.function(
    image=image,
    volumes={"/data": volume},
    timeout=600
)
@modal.fastapi_endpoint(method="POST")
async def upload_bin(request):
    """Upload and analyze a .bin file"""
    from fastapi import UploadFile, File
    from fastapi.responses import JSONResponse
    
    form = await request.form()
    file: UploadFile = form.get("file")
    
    if not file:
        return JSONResponse({"error": "No file provided"}, status_code=400)
    
    # Read file content
    content = await file.read()
    
    # Generate unique ID for this upload
    file_id = hashlib.md5(content).hexdigest()
    
    # Save to volume
    file_path = f"/data/{file_id}.bin"
    with open(file_path, "wb") as f:
        f.write(content)
    
    volume.commit()
    
    # Extract files
    extracted_files = extract_binary_file(content)
    
    # Store metadata
    metadata_path = f"/data/{file_id}.json"
    metadata = {
        'file_id': file_id,
        'original_name': file.filename,
        'size': len(content),
        'extracted_count': len(extracted_files),
        'files': [
            {
                'name': f['name'],
                'size': f['size'],
                'offset': f['offset'],
                'type': f['type']
            }
            for f in extracted_files
        ]
    }
    
    with open(metadata_path, "w") as f:
        json.dump(metadata, f)
    
    volume.commit()
    
    return JSONResponse({
        'file_id': file_id,
        'metadata': metadata
    })


@app.function(
    image=image,
    volumes={"/data": volume},
    timeout=600
)
@modal.fastapi_endpoint(method="POST")
async def create_zip(request):
    """Create a ZIP file from selected files with streaming progress"""
    from fastapi.responses import StreamingResponse
    import asyncio
    
    body = await request.json()
    file_id = body.get('file_id')
    selected_indices = body.get('selected_files', [])
    
    async def progress_generator():
        try:
            # Load metadata
            metadata_path = f"/data/{file_id}.json"
            with open(metadata_path, "r") as f:
                metadata = json.load(f)
            
            # Load original binary
            bin_path = f"/data/{file_id}.bin"
            with open(bin_path, "rb") as f:
                bin_content = f.read()
            
            yield f"data: {json.dumps({'type': 'info', 'message': 'Extracting files from binary...'})}\n\n"
            await asyncio.sleep(0.1)
            
            # Extract files
            extracted_files = extract_binary_file(bin_content)
            
            yield f"data: {json.dumps({'type': 'info', 'message': f'Found {len(extracted_files)} files'})}\n\n"
            await asyncio.sleep(0.1)
            
            # Create ZIP in memory
            zip_buffer = io.BytesIO()
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                total_files = len(selected_indices)
                
                for idx, file_idx in enumerate(selected_indices):
                    if file_idx < len(extracted_files):
                        file_info = extracted_files[file_idx]
                        
                        progress = int((idx / total_files) * 100)
                        yield f"data: {json.dumps({'type': 'progress', 'value': progress, 'message': f'Adding {file_info['name']}'})}\n\n"
                        await asyncio.sleep(0.05)
                        
                        # Add file to ZIP
                        zf.writestr(file_info['name'], file_info['data'])
            
            yield f"data: {json.dumps({'type': 'progress', 'value': 100, 'message': 'ZIP creation complete!'})}\n\n"
            await asyncio.sleep(0.1)
            
            # Save ZIP to volume
            zip_path = f"/data/{file_id}.zip"
            zip_buffer.seek(0)
            with open(zip_path, "wb") as f:
                f.write(zip_buffer.getvalue())
            
            volume.commit()
            
            yield f"data: {json.dumps({'type': 'complete', 'file_id': file_id, 'size': len(zip_buffer.getvalue())})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        progress_generator(),
        media_type="text/event-stream"
    )


@app.function(
    image=image,
    volumes={"/data": volume}
)
@modal.fastapi_endpoint(method="GET")
async def download_zip(file_id: str):
    """Download the created ZIP file"""
    from fastapi.responses import FileResponse
    
    zip_path = f"/data/{file_id}.zip"
    
    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=f"extracted_{file_id}.zip"
    )


@app.function(image=image)
@modal.fastapi_endpoint(method="GET")
async def health():
    """Health check endpoint"""
    from fastapi.responses import JSONResponse
    return JSONResponse({"status": "ok", "service": "bin-extractor"})