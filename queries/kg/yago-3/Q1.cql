MATCH (v0)
WHERE v0.`http://schema.org/image` IS NOT NULL 
RETURN v0.`http://schema.org/duration`