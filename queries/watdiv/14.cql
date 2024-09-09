MATCH (v0)-[:`http://purl.org/goodrelations/includes`]->(v1),
(v101)-[:`http://purl.org/goodrelations/offers`]->(v0),
(v0)-[:`http://schema.org/eligibleRegion`]->(v8)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Retailer3" AND 
v0.`http://purl.org/goodrelations/price` IS NOT NULL AND 
v0.`http://purl.org/goodrelations/serialNumber` IS NOT NULL AND 
v0.`http://purl.org/goodrelations/validFrom` IS NOT NULL AND 
v0.`http://purl.org/goodrelations/validThrough` IS NOT NULL AND 
v0.`http://schema.org/eligibleQuantity` IS NOT NULL AND 
v0.`http://schema.org/priceValidUntil` IS NOT NULL
RETURN v0.id, v1.id, v0.`http://purl.org/goodrelations/price`, v0.`http://purl.org/goodrelations/serialNumber`, v0.`http://purl.org/goodrelations/validFrom`, v0.`http://purl.org/goodrelations/validThrough`, v0.`http://schema.org/eligibleQuantity`, v8.id, v0.`http://schema.org/priceValidUntil`