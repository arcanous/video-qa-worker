import logging
from typing import List, Dict, Any
from openai import OpenAI
from .util import chunk_text_by_tokens

logger = logging.getLogger("video_worker")


def generate_embeddings(text: str, video_id: int, model: str = "text-embedding-3-small") -> List[float]:
    """
    Generate embeddings for text using OpenAI
    
    Returns:
        List of embedding values (1536 dimensions)
    """
    try:
        client = OpenAI()
        
        logger.debug(f"Generating embeddings for video {video_id} (model: {model})")
        
        response = client.embeddings.create(
            model=model,
            input=text,
            dimensions=1536
        )
        
        embedding = response.data[0].embedding
        
        logger.debug(f"Generated embedding for video {video_id}: {len(embedding)} dimensions")
        return embedding
        
    except Exception as e:
        error_msg = f"Error generating embeddings for video {video_id}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def embed_transcript_segments(segments: List[Dict[str, Any]], video_id: int) -> List[Dict[str, Any]]:
    """
    Generate embeddings for transcript segments
    
    Returns:
        List of segments with embeddings
    """
    try:
        logger.info(f"Embedding transcript segments for video {video_id}: {len(segments)} segments")
        
        embedded_segments = []
        
        for segment in segments:
            text = segment.get('text', '')
            if not text.strip():
                continue
            
            try:
                embedding = generate_embeddings(text, video_id)
                
                embedded_segment = segment.copy()
                embedded_segment['embedding'] = embedding
                embedded_segments.append(embedded_segment)
                
            except Exception as e:
                logger.warning(f"Error embedding segment for video {video_id}: {str(e)}")
                continue
        
        logger.info(f"Embedded {len(embedded_segments)} transcript segments for video {video_id}")
        return embedded_segments
        
    except Exception as e:
        error_msg = f"Error embedding transcript segments for video {video_id}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def embed_transcript_by_scenes(segments: List[Dict[str, Any]], scenes: List[Dict[str, Any]], video_id: int) -> List[Dict[str, Any]]:
    """
    Generate embeddings for transcript chunks grouped by scenes
    
    Returns:
        List of scene-based transcript chunks with embeddings
    """
    try:
        logger.info(f"Embedding transcript by scenes for video {video_id}")
        
        # Group segments by scenes
        scene_chunks = []
        
        for scene in scenes:
            scene_idx = scene['idx']
            scene_start = scene['t_start']
            scene_end = scene['t_end']
            
            # Find segments that overlap with this scene
            scene_segments = []
            for segment in segments:
                if (segment['t_start'] < scene_end and segment['t_end'] > scene_start):
                    scene_segments.append(segment)
            
            if scene_segments:
                # Combine text from overlapping segments
                combined_text = ' '.join(seg['text'] for seg in scene_segments)
                
                # Chunk if too long
                chunks = chunk_text_by_tokens(combined_text, max_tokens=500, overlap=50)
                
                for i, chunk in enumerate(chunks):
                    try:
                        embedding = generate_embeddings(chunk, video_id)
                        
                        scene_chunks.append({
                            'scene_idx': scene_idx,
                            't_start': scene_start,
                            't_end': scene_end,
                            'text': chunk,
                            'embedding': embedding,
                            'chunk_idx': i
                        })
                        
                    except Exception as e:
                        logger.warning(f"Error embedding scene chunk {i} for video {video_id}: {str(e)}")
                        continue
        
        logger.info(f"Embedded {len(scene_chunks)} scene-based transcript chunks for video {video_id}")
        return scene_chunks
        
    except Exception as e:
        error_msg = f"Error embedding transcript by scenes for video {video_id}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def embed_frame_captions(frame_analyses: List[Dict[str, Any]], video_id: int) -> List[Dict[str, Any]]:
    """
    Generate embeddings for frame captions and analysis
    
    Returns:
        List of frame analyses with embeddings
    """
    try:
        logger.info(f"Embedding frame captions for video {video_id}: {len(frame_analyses)} frames")
        
        embedded_frames = []
        
        for frame_analysis in frame_analyses:
            analysis = frame_analysis.get('analysis', {})
            caption = analysis.get('caption', '')
            
            if not caption.strip():
                continue
            
            try:
                # Create rich text for embedding
                embed_text = f"Caption: {caption}"
                
                # Add controls information
                controls = analysis.get('controls', [])
                if controls:
                    controls_text = "Controls: " + "; ".join([
                        f"{c.get('label', '')} ({c.get('kind', '')}): {c.get('reading', '')} {c.get('units', '')}"
                        for c in controls
                    ])
                    embed_text += f" {controls_text}"
                
                # Add text on screen
                text_on_screen = analysis.get('text_on_screen', [])
                if text_on_screen:
                    screen_text = "Text on screen: " + "; ".join([
                        t.get('text', '') for t in text_on_screen
                    ])
                    embed_text += f" {screen_text}"
                
                embedding = generate_embeddings(embed_text, video_id)
                
                embedded_frame = frame_analysis.copy()
                embedded_frame['embedding'] = embedding
                embedded_frame['embed_text'] = embed_text
                embedded_frames.append(embedded_frame)
                
            except Exception as e:
                logger.warning(f"Error embedding frame caption for video {video_id}: {str(e)}")
                continue
        
        logger.info(f"Embedded {len(embedded_frames)} frame captions for video {video_id}")
        return embedded_frames
        
    except Exception as e:
        error_msg = f"Error embedding frame captions for video {video_id}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def batch_embed_texts(texts: List[str], video_id: int, batch_size: int = 10) -> List[List[float]]:
    """
    Generate embeddings for multiple texts in batches
    
    Returns:
        List of embedding vectors
    """
    try:
        logger.info(f"Batch embedding {len(texts)} texts for video {video_id}")
        
        all_embeddings = []
        
        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i + batch_size]
            
            try:
                client = OpenAI()
                
                response = client.embeddings.create(
                    model="text-embedding-3-small",
                    input=batch_texts,
                    dimensions=1536
                )
                
                batch_embeddings = [data.embedding for data in response.data]
                all_embeddings.extend(batch_embeddings)
                
                logger.debug(f"Embedded batch {i//batch_size + 1} for video {video_id}")
                
            except Exception as e:
                logger.warning(f"Error embedding batch {i//batch_size + 1} for video {video_id}: {str(e)}")
                # Add empty embeddings for failed batch
                all_embeddings.extend([[] for _ in batch_texts])
        
        logger.info(f"Batch embedding completed for video {video_id}: {len(all_embeddings)} embeddings")
        return all_embeddings
        
    except Exception as e:
        error_msg = f"Error in batch embedding for video {video_id}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def validate_embedding(embedding: List[float]) -> bool:
    """Validate embedding vector"""
    if not embedding:
        return False
    
    if len(embedding) != 1536:
        logger.warning(f"Invalid embedding dimension: {len(embedding)}, expected 1536")
        return False
    
    # Check for all zeros or NaN
    if all(x == 0 for x in embedding):
        logger.warning("Embedding is all zeros")
        return False
    
    if any(x != x for x in embedding):  # Check for NaN
        logger.warning("Embedding contains NaN values")
        return False
    
    return True
