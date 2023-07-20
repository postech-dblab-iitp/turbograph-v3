/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/filesystem.hpp>

#include <thrift/compiler/ast/ast_visitor.h>
#include <thrift/compiler/ast/t_include.h>
#include <thrift/compiler/ast/t_type.h>
#include <thrift/compiler/generate/t_generator.h>
#include <thrift/compiler/lib/const_util.h>
#include <thrift/compiler/lib/schematizer.h>

#include <thrift/lib/cpp2/protocol/DebugProtocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/protocol/SimpleJSONProtocol.h>
#include <thrift/lib/thrift/gen-cpp2/ast_types_custom_protocol.h>
#include <thrift/lib/thrift/gen-cpp2/ast_visitation.h>

namespace apache {
namespace thrift {
namespace compiler {

enum class ast_protocol {
  json,
  debug,
};

template <typename Writer, typename T>
std::string serialize(const T& val) {
  folly::IOBufQueue queue;
  Writer proto;
  proto.setOutput(&queue);
  op::encode<type::struct_t<T>>(proto, val);
  auto buf = queue.move();
  auto br = buf->coalesce();
  return std::string(reinterpret_cast<const char*>(br.data()), br.size());
}
/**
 * AST generator.
 */
class t_ast_generator : public t_generator {
 public:
  using t_generator::t_generator;

  void process_options(
      const std::map<std::string, std::string>& options) override {
    out_dir_base_ = "gen-ast";
    protocol_ = ast_protocol::debug;
    for (auto& pair : options) {
      if (pair.first == "protocol") {
        if (pair.second == "debug") {
          protocol_ = ast_protocol::debug;
        } else if (pair.second == "json") {
          protocol_ = ast_protocol::json;
          throw std::runtime_error("TODO");
        } else {
          throw std::runtime_error(
              fmt::format("Unknown protocol `{}`", pair.second));
        }
      } else if (pair.first == "ast") {
      } else {
        throw std::runtime_error(
            fmt::format("Unknown option `{}`", pair.first));
      }
    }
  }

  void generate_program() override;

 private:
  std::ofstream f_out_;
  ast_protocol protocol_;
};

void t_ast_generator::generate_program() {
  boost::filesystem::create_directory(get_out_dir());
  std::string fname = fmt::format("{}/{}.ast", get_out_dir(), program_->name());
  f_out_.open(fname.c_str());

  cpp2::AST ast;
  std::unordered_map<const t_program*, apache::thrift::type::ProgramId>
      program_index;
  std::unordered_map<const t_named*, apache::thrift::type::DefinitionId>
      definition_index;
  const_ast_visitor visitor;
  visitor.add_program_visitor([&](const t_program& program) {
    if (program_index.count(&program)) {
      return;
    }

    auto& programs = *ast.programs();
    auto pos = programs.size();
    program_index[&program] =
        static_cast<apache::thrift::type::ProgramId>(pos + 1);
    hydrate_const(programs.emplace_back(), *schematizer::gen_schema(program));

    for (auto* include : program.get_included_programs()) {
      // This could invalidate references into `programs`.
      visitor(*include);
      programs.at(pos).includes().ensure().push_back(program_index.at(include));
      auto& defs = programs.at(pos).definitions().ensure();
      for (auto& def : include->definitions()) {
        defs.push_back(definition_index.at(&def));
      }
    }

    // TODO: sourceInfo
    // TODO: languageIncludes

    // Note: have to populate `definitions` after the visitor completes since it
    // visits the children after this lambda returns.
  });

#define THRIFT_ADD_VISITOR(kind)                                  \
  visitor.add_##kind##_visitor([&](const t_##kind& node) {        \
    if (node.generated()) {                                       \
      return;                                                     \
    }                                                             \
    auto& definitions = *ast.definitions();                       \
    auto pos = definitions.size();                                \
    definition_index[&node] =                                     \
        static_cast<apache::thrift::type::DefinitionId>(pos + 1); \
    hydrate_const(                                                \
        definitions.emplace_back().kind##Def_ref().ensure(),      \
        *schematizer::gen_schema(node));                          \
  })
  THRIFT_ADD_VISITOR(service);
  THRIFT_ADD_VISITOR(interaction);
  THRIFT_ADD_VISITOR(struct);
  THRIFT_ADD_VISITOR(union);
  THRIFT_ADD_VISITOR(exception);
  THRIFT_ADD_VISITOR(typedef);
  THRIFT_ADD_VISITOR(enum);
  THRIFT_ADD_VISITOR(const);
#undef THRIFT_ADD_VISITOR
  visitor(*program_);
  auto& defs = ast.programs()->at(0).definitions().ensure();
  for (auto& def : program_->definitions()) {
    defs.push_back(definition_index.at(&def));
  }

  f_out_ << serialize<DebugProtocolWriter>(ast);
  // TODO: JSON
  f_out_.close();
}

THRIFT_REGISTER_GENERATOR(
    ast,
    "AST",
    "    protocol:        Which of [json|debug] protocols to use for serialization.\n");

} // namespace compiler
} // namespace thrift
} // namespace apache
