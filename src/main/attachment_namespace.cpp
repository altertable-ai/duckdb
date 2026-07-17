#include "duckdb/main/attachment_namespace.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

idx_t AttachmentNamespace::Attach(const string &alias, shared_ptr<AttachedDatabase> database, AccessMode access_mode,
                                  AttachVisibility visibility) {
	if (!database) {
		throw InternalException("AttachmentNamespace::Attach called with null database");
	}
	if (alias.empty()) {
		throw InternalException("AttachmentNamespace::Attach called with empty alias");
	}
	lock_guard<mutex> guard(lock);
	AttachmentBinding binding;
	binding.alias = alias;
	binding.database = std::move(database);
	binding.access_mode = access_mode;
	binding.generation = next_generation++;
	binding.visibility = visibility;
	auto generation = binding.generation;
	bindings[alias] = std::move(binding);
	return generation;
}

shared_ptr<AttachedDatabase> AttachmentNamespace::Detach(const string &alias) {
	lock_guard<mutex> guard(lock);
	auto entry = bindings.find(alias);
	if (entry == bindings.end()) {
		return nullptr;
	}
	auto database = entry->second.database;
	bindings.erase(entry);
	return database;
}

bool AttachmentNamespace::TryGetBinding(const string &alias, AttachmentBinding &out) const {
	lock_guard<mutex> guard(lock);
	auto entry = bindings.find(alias);
	if (entry == bindings.end()) {
		return false;
	}
	out = entry->second;
	return true;
}

shared_ptr<AttachedDatabase> AttachmentNamespace::GetDatabase(const string &alias) const {
	lock_guard<mutex> guard(lock);
	auto entry = bindings.find(alias);
	if (entry == bindings.end()) {
		return nullptr;
	}
	return entry->second.database;
}

vector<AttachmentBinding> AttachmentNamespace::List() const {
	lock_guard<mutex> guard(lock);
	vector<AttachmentBinding> result;
	result.reserve(bindings.size());
	for (auto &entry : bindings) {
		result.push_back(entry.second);
	}
	return result;
}

void AttachmentNamespace::CopyTo(AttachmentNamespace &other) const {
	auto snapshot = List();
	lock_guard<mutex> guard(other.lock);
	for (auto &binding : snapshot) {
		AttachmentBinding copy = binding;
		copy.generation = other.next_generation++;
		other.bindings[copy.alias] = std::move(copy);
	}
}

vector<shared_ptr<AttachedDatabase>> AttachmentNamespace::DetachAll() {
	lock_guard<mutex> guard(lock);
	vector<shared_ptr<AttachedDatabase>> result;
	result.reserve(bindings.size());
	for (auto &entry : bindings) {
		result.push_back(entry.second.database);
	}
	bindings.clear();
	return result;
}

bool AttachmentNamespace::Empty() const {
	lock_guard<mutex> guard(lock);
	return bindings.empty();
}

idx_t AttachmentNamespace::Count() const {
	lock_guard<mutex> guard(lock);
	return bindings.size();
}

} // namespace duckdb
