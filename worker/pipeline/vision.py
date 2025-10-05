import base64
import json
import logging
import asyncio
import os
import time
from typing import Dict, Any, List
from openai import OpenAI, AsyncOpenAI
from pydantic import BaseModel, Field

logger = logging.getLogger("video_worker")


class ControlItem(BaseModel):
    """Control item from vision analysis"""
    label: str = Field(description="Label or name of the control")
    kind: str = Field(description="Type of control (button, dial, switch, etc.)")
    reading: str = Field(description="Current reading or value")
    units: str = Field(description="Units of measurement if applicable")


class TextOnScreen(BaseModel):
    """Text detected on screen"""
    text: str = Field(description="The text content")
    confidence: float = Field(description="Confidence score 0-1", ge=0, le=1)


class VisionAnalysis(BaseModel):
    """Structured output from vision analysis"""
    caption: str = Field(description="Detailed caption describing the scene")
    controls: List[ControlItem] = Field(description="List of controls visible in the image")
    text_on_screen: List[TextOnScreen] = Field(description="Text visible on screen")


def analyze_frame_with_vision(frame_path: str, video_id: str) -> Dict[str, Any]:
    """
    Analyze frame using OpenAI GPT-4o Vision with structured outputs
    
    Returns:
        Dictionary with caption, controls, and text_on_screen
    """
    try:
        client = OpenAI()
        
        logger.info(f"Analyzing frame with vision for video {video_id}: {frame_path}")
        
        # Encode image to base64
        with open(frame_path, 'rb') as image_file:
            base64_image = base64.b64encode(image_file.read()).decode('utf-8')
        
        # Create structured output schema with additionalProperties: false
        base_schema = VisionAnalysis.model_json_schema()
        
        # Add additionalProperties: false to all object definitions
        def add_additional_properties_false(schema_part):
            if isinstance(schema_part, dict):
                if schema_part.get("type") == "object":
                    schema_part["additionalProperties"] = False
                for key, value in schema_part.items():
                    add_additional_properties_false(value)
            elif isinstance(schema_part, list):
                for item in schema_part:
                    add_additional_properties_false(item)
        
        add_additional_properties_false(base_schema)
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": """Analyze this image and provide a detailed description. Focus on:
1. What is happening in the scene
2. Any controls, buttons, dials, or interfaces visible
3. Any text or labels that appear on screen
4. The overall context and setting

Be thorough and accurate in your analysis."""
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}"
                            }
                        }
                    ]
                }
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "vision_analysis",
                    "schema": base_schema,
                    "strict": True
                }
            },
            temperature=0.1
        )
        
        # Parse the structured response
        content = response.choices[0].message.content
        analysis_data = json.loads(content)
        
        # Validate and convert to our format
        analysis = VisionAnalysis(**analysis_data)
        
        result = {
            'caption': analysis.caption,
            'controls': [
                {
                    'label': control.label,
                    'kind': control.kind,
                    'reading': control.reading,
                    'units': control.units
                }
                for control in analysis.controls
            ],
            'text_on_screen': [
                {
                    'text': text.text,
                    'confidence': text.confidence
                }
                for text in analysis.text_on_screen
            ]
        }
        
        logger.info(f"Vision analysis completed for video {video_id}: {len(result['controls'])} controls, {len(result['text_on_screen'])} text items")
        
        return result
        
    except Exception as e:
        error_msg = f"Error analyzing frame with vision for video {video_id}: {str(e)}"
        logger.error(error_msg)
        # Return minimal structure on error
        return {
            'caption': f"Error analyzing frame: {str(e)}",
            'controls': [],
            'text_on_screen': []
        }


async def analyze_frame_with_vision_async(
    frame_path: str, 
    video_id: str,
    semaphore: asyncio.Semaphore
) -> Dict[str, Any]:
    """
    Async version of analyze_frame_with_vision for parallel processing
    
    Args:
        frame_path: Path to the frame image
        video_id: Video identifier
        semaphore: Semaphore to control concurrent requests
    
    Returns:
        Dictionary with caption, controls, and text_on_screen
    """
    async with semaphore:
        try:
            client = AsyncOpenAI()
            
            logger.debug(f"Analyzing frame with vision for video {video_id}: {frame_path}")
            
            # Encode image to base64
            with open(frame_path, 'rb') as image_file:
                base64_image = base64.b64encode(image_file.read()).decode('utf-8')
            
            # Create structured output schema with additionalProperties: false
            base_schema = VisionAnalysis.model_json_schema()
            
            # Add additionalProperties: false to all object definitions
            def add_additional_properties_false(schema_part):
                if isinstance(schema_part, dict):
                    if schema_part.get("type") == "object":
                        schema_part["additionalProperties"] = False
                    for key, value in schema_part.items():
                        add_additional_properties_false(value)
                elif isinstance(schema_part, list):
                    for item in schema_part:
                        add_additional_properties_false(item)
            
            add_additional_properties_false(base_schema)
            
            response = await client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": """Analyze this image and provide a detailed description. Focus on:
1. What is happening in the scene
2. Any controls, buttons, dials, or interfaces visible
3. Any text or labels that appear on screen
4. The overall context and setting

Be thorough and accurate in your analysis."""
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_image}"
                                }
                            }
                        ]
                    }
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "vision_analysis",
                        "schema": base_schema,
                        "strict": True
                    }
                },
                temperature=0.1
            )
            
            # Parse the structured response
            content = response.choices[0].message.content
            analysis_data = json.loads(content)
            
            # Validate and convert to our format
            analysis = VisionAnalysis(**analysis_data)
            
            result = {
                'caption': analysis.caption,
                'controls': [
                    {
                        'label': control.label,
                        'kind': control.kind,
                        'reading': control.reading,
                        'units': control.units
                    }
                    for control in analysis.controls
                ],
                'text_on_screen': [
                    {
                        'text': text.text,
                        'confidence': text.confidence
                    }
                    for text in analysis.text_on_screen
                ]
            }
            
            logger.debug(f"Vision analysis completed for video {video_id}: {len(result['controls'])} controls, {len(result['text_on_screen'])} text items")
            
            return result
            
        except Exception as e:
            error_msg = f"Error analyzing frame with vision for video {video_id}: {str(e)}"
            logger.error(error_msg)
            # Return minimal structure on error
            return {
                'caption': f"Error analyzing frame: {str(e)}",
                'controls': [],
                'text_on_screen': []
            }


async def batch_analyze_frames_parallel(
    frames: List[Dict[str, Any]], 
    video_id: str,
    max_concurrent: int = 5
) -> List[Dict[str, Any]]:
    """
    Analyze multiple frames in parallel using asyncio
    
    Args:
        frames: List of frame dictionaries
        video_id: Video identifier
        max_concurrent: Max concurrent API requests (default: 5)
    
    Returns:
        List of analysis results with frame metadata
    """
    start_time = time.time()
    
    logger.info(f"Parallel processing {len(frames)} frames for video {video_id} with {max_concurrent} concurrent requests")
    
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)
    
    # Create async task for each frame
    async def analyze_frame_async(frame_data: Dict[str, Any], frame_idx: int) -> Dict[str, Any]:
        """Analyze a single frame asynchronously"""
        try:
            analysis = await analyze_frame_with_vision_async(
                frame_data['path'], 
                video_id, 
                semaphore
            )
            
            return {
                'frame_id': frame_idx,
                'scene_idx': frame_data['scene_idx'],
                'path': frame_data['path'],
                'timestamp': frame_data.get('timestamp', 0.0),
                'phash': frame_data['phash'],
                'analysis': analysis
            }
            
        except Exception as e:
            logger.warning(f"Error analyzing frame {frame_idx} for video {video_id}: {str(e)}")
            return {
                'frame_id': frame_idx,
                'scene_idx': frame_data['scene_idx'],
                'path': frame_data['path'],
                'timestamp': frame_data.get('timestamp', 0.0),
                'phash': frame_data['phash'],
                'analysis': {
                    'caption': f"Analysis failed: {str(e)}",
                    'controls': [],
                    'text_on_screen': []
                }
            }
    
    # Create tasks for all frames
    tasks = [
        analyze_frame_async(frame, i) 
        for i, frame in enumerate(frames)
    ]
    
    # Execute all tasks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Handle any exceptions that occurred
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Task {i} failed with exception: {str(result)}")
            # Create error result
            processed_results.append({
                'frame_id': i,
                'scene_idx': frames[i]['scene_idx'],
                'path': frames[i]['path'],
                'timestamp': frames[i].get('timestamp', 0.0),
                'phash': frames[i]['phash'],
                'analysis': {
                    'caption': f"Task failed: {str(result)}",
                    'controls': [],
                    'text_on_screen': []
                }
            })
        else:
            processed_results.append(result)
    
    elapsed = time.time() - start_time
    avg_time_per_frame = elapsed / len(frames) if frames else 0
    
    logger.info(f"Parallel processing completed for video {video_id} in {elapsed:.2f}s (avg {avg_time_per_frame:.2f}s per frame)")
    
    return processed_results


def batch_analyze_frames(frames: List[Dict[str, Any]], video_id: str) -> List[Dict[str, Any]]:
    """
    Analyze multiple frames and return results with frame metadata
    
    This function now uses parallel processing by default, with fallback to sequential
    if parallel processing fails.
    
    Returns:
        List of analysis results with frame info
    """
    # Get max concurrent from environment variable
    max_concurrent = int(os.getenv("VISION_MAX_CONCURRENT", "5"))
    
    try:
        # Try parallel processing first
        return asyncio.run(batch_analyze_frames_parallel(frames, video_id, max_concurrent))
    except Exception as e:
        logger.warning(f"Parallel processing failed for video {video_id}, falling back to sequential: {str(e)}")
        
        # Fallback to sequential processing
        results = []
        
        logger.info(f"Sequential analyzing {len(frames)} frames for video {video_id}")
        
        for i, frame in enumerate(frames):
            try:
                analysis = analyze_frame_with_vision(frame['path'], video_id)
                
                result = {
                    'frame_id': i,
                    'scene_idx': frame['scene_idx'],
                    'path': frame['path'],
                    'timestamp': frame.get('timestamp', 0.0),
                    'phash': frame['phash'],
                    'analysis': analysis
                }
                
                results.append(result)
                
                logger.debug(f"Analyzed frame {i+1}/{len(frames)} for video {video_id}")
                
            except Exception as e:
                logger.warning(f"Error analyzing frame {i} for video {video_id}: {str(e)}")
                # Add error result
                results.append({
                    'frame_id': i,
                    'scene_idx': frame['scene_idx'],
                    'path': frame['path'],
                    'timestamp': frame.get('timestamp', 0.0),
                    'phash': frame['phash'],
                    'analysis': {
                        'caption': f"Analysis failed: {str(e)}",
                        'controls': [],
                        'text_on_screen': []
                    }
                })
        
        logger.info(f"Sequential analysis completed for video {video_id}: {len(results)} results")
        return results


def validate_vision_analysis(analysis: Dict[str, Any]) -> bool:
    """Validate vision analysis results"""
    required_fields = ['caption', 'controls', 'text_on_screen']
    
    for field in required_fields:
        if field not in analysis:
            return False
    
    # Validate controls structure
    for control in analysis.get('controls', []):
        if not all(key in control for key in ['label', 'kind', 'reading', 'units']):
            return False
    
    # Validate text_on_screen structure
    for text in analysis.get('text_on_screen', []):
        if not all(key in text for key in ['text', 'confidence']):
            return False
        if not (0 <= text.get('confidence', 0) <= 1):
            return False
    
    return True


def extract_key_entities(analysis: Dict[str, Any]) -> List[str]:
    """Extract key entities from vision analysis"""
    entities = []
    
    # Extract from caption
    caption = analysis.get('caption', '')
    # Simple entity extraction - could be enhanced with NER
    words = caption.lower().split()
    entities.extend([word for word in words if len(word) > 3])
    
    # Extract control labels
    for control in analysis.get('controls', []):
        entities.append(control.get('label', '').lower())
    
    # Extract text on screen
    for text in analysis.get('text_on_screen', []):
        entities.append(text.get('text', '').lower())
    
    # Remove duplicates and empty strings
    entities = list(set([e for e in entities if e.strip()]))
    
    return entities
