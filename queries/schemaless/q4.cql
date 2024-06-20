MATCH (n) WHERE n.firstName = "Marc" RETURN n.firstName;
MATCH (n) WHERE n.creationDate <= 1264060800000 RETURN n.creationDate;
MATCH (n) WHERE n.name = "Europe" RETURN n.name;
MATCH (n) WHERE n.firstName = "Marc" and n.creationDate <= 1264060800000 RETURN n.firstName, n.creationDate;
MATCH (n) WHERE n.name = "Europe" and n.url = "141.223.221.11" RETURN n.name, n.url;
MATCH (n) WHERE n.browserUsed = "Firefox" and n.locationIP = "141.223.221.11" RETURN n.browserUsed, n.locationIP;

MATCH (n:Person) WHERE n.firstName = "Marc" RETURN n.firstName;
MATCH (n:Person) WHERE n.creationDate <= 1264060800000 RETURN n.creationDate;
MATCH (n:Organisation) WHERE n.name = "Europe" RETURN n.name;
MATCH (n:Person) WHERE n.firstName = "Marc" and n.creationDate <= 1264060800000 RETURN n.firstName, n.creationDate;
MATCH (n:Organisation) WHERE n.name = "Europe" and n.url = "141.223.221.11" RETURN n.name, n.url;
MATCH (n:Comment) WHERE n.browserUsed = "Firefox" and n.locationIP = "141.223.221.11" RETURN n.browserUsed, n.locationIP;