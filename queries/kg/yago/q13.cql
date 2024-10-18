MATCH (v0)-[:`http://schema.org/about`]->(v1), (v0)-[:`http://schema.org/about`]->(v2) WHERE v1.`http://schema.org/mainEntityOfPage` IS NOT NULL RETURN v0.uri, v1.uri, v2.uri

MATCH (v0)-[:`http://schema.org/about`]->(v1), (v0)-[:`http://schema.org/about`]->(v2) WHERE v1.`http://schema.org/mainEntityOfPage` IS NOT NULL RETURN COUNT(*)

MATCH (v1) WHERE v1.`http://schema.org/mainEntityOfPage` IS NOT NULL RETURN v1.uri