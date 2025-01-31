#pragma once

/*
 * Design of the ConfigurationProperties
 *
 * This class provides functions to store and read configuration property values.
 * For each configuration property, AddProperty() insert property value into map
 * data structure. The HasKey() function checks whether the value corresponding 
 * to the property key given as input is stored in the map. If it exists, 
 * GetProperty() retrieves the property value from the map and returns it.
 */

#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <sstream>
#include <vector>

enum class empties_t { empties_ok, no_empties };

template <typename Container>
Container& split(Container& result, const typename Container::value_type& s,
                 typename Container::value_type::value_type delimiter,
                 empties_t empties = empties_t::empties_ok ) {
	result.clear();
	std::istringstream ss(s);
	while (!ss.eof()) {
		typename Container::value_type field;
		getline(ss, field, delimiter);
		if ((empties == empties_t::no_empties) && field.empty()) continue;
		result.push_back(field);
	}
	return result;
}

class ConfigProperties {
  public:
	ConfigProperties() {
		mapping.clear();
	}

	void AddProperty(const std::string &pair) {
		std::vector<std::string> key_val;
		split(key_val, pair, '=', empties_t::no_empties);
		this->mapping.insert(std::make_pair(key_val[0], key_val[1]));
	}

	// Read every kv pairs (K=V) within a string; "K1=V1,K2=V2, ..."
	void AddProperties(const std::string &opts, char delimiter) {
		std::vector<std::string> val;
		for (const std::string& pair : split(val, opts, delimiter, empties_t::no_empties)) {
			std::vector<std::string> key_val;
			split(key_val, pair, '=', empties_t::no_empties);
			this->mapping.insert(std::make_pair(key_val[0], key_val[1]));
		}
	}

	std::string GetProperty(const std::string& name) const {
		std::map<std::string, std::string>::const_iterator it = mapping.find(name);
		if(it != mapping.end()) {
			return it->second;
		} else {
			return "NULL";
		}
	}

	bool HasKey(const std::string& x) const {
		return mapping.count(x) > 0;
	}

	std::string ReadProperty(const std::string &name) const {
		return GetProperty(name);
	}

	bool ReadProperty(const std::string &name, std::string &value) const {
		if (HasKey(name)) {
			value = GetProperty(name);
			return true;
		} else
			return false;
	}

	bool ReadPropertyInt(const std::string &name, int &value) const {
		if (HasKey(name)) {
			value = std::stoi(GetProperty(name));
			return true;
		} else
			return false;
	}

	bool ReadPropertyLong(const std::string &name, long &value) const {
		if (HasKey(name)) {
			value = std::stol(GetProperty(name));
			return true;
		} else
			return false;
	}

	bool ReadPropertyBool(const std::string &name, bool &value) const {
		if (HasKey(name)) {
			value = (bool)(std::stoi(GetProperty((name))));
			return true;
		} else
			return false;
	}

	bool ReadPropertyDouble(const std::string &name, double &value) const {
		if (HasKey(name)) {
			value = std::stod(GetProperty(name));
			return true;
		} else
			return false;
	}
	const std::map<std::string, std::string> &ReadProperties() const {
		return mapping;
	}

  private:
	std::map <std::string, std::string> mapping;
};
