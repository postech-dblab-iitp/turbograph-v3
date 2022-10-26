// Author: Ozan Irsoy
// https://github.com/oir/tblr/blob/main/tblr.h

#ifndef TBLR_H
#define TBLR_H

#include <algorithm>
#include <cassert>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace tblr {

/** Alignment (justification) of text within a table cell. */
enum Align : char { Left = 'l', Center = 'c', Right = 'r' };

/// * SingleLine - Do not split cell text into lines
/// * Naive      - Split cell text into lines exactly by column width
/// * Space      - Split cell text into lines while trying to avoid consecutive
///                non-whitespace chars
enum LineSplitter { SingleLine, Naive, Space };

/// Vector of strings that constitute a row of a table. Each element
/// belongs to a cell.
using Row = std::vector<std::string>;

/// Vector of widths for each column of a table. A zero width value
/// means automatic: width is determined by the longest cell in that
/// column.
using Widths = std::vector<size_t>;

/** Vector of alignments per column. See Align. */
using Aligns = std::vector<Align>;

/** Class to end a row */
class Endr {};
const static Endr endr, endl;

/** Is first byte of a UTF8 character */
inline bool is_first_byte(const char& c) {
  // https://stackoverflow.com/a/4063229
  return (c & 0xc0) != 0x80;
}

/** UTF8 length of a string */
inline size_t len(const std::string& s) {
  return std::count_if(s.begin(), s.end(), is_first_byte);
}

/** UTF8 aware substring */
inline std::string
substr(const std::string& s, size_t left = 0, size_t size = -1) {
  auto i = s.begin();
  for (left++; i != s.end() and (left -= is_first_byte(*i)); i++) {}
  auto pos = i;
  for (size++; i != s.end() and (size -= is_first_byte(*i)); i++) {}
  return s.substr(pos - s.begin(), i - pos);
}

/** Replace all appearances of "\r", "\r\n" with "\n" */
std::string normalize_newlines(const std::string& s) {
  std::string rval;
  rval.reserve(s.size());

  for (size_t i = 0; i < s.size(); i++) {
    if (s[i] == '\r') {
      if (i < (s.size() - 1) and s[i + 1] == '\n') { i++; }
      rval += '\n';
    } else {
      rval += s[i];
    }
  }

  return rval;
}

/// Helper class to use stream operator (<<) without moving to the next column,
/// piping into the same cell of the table.
class Cell {
 private:
  std::stringstream ss_;

 public:
  template <typename T>
  friend Cell& operator<<(Cell& c, const T& x) {
    c.ss_ << x;
    return c;
  }
  template <typename T>
  friend Cell&& operator<<(Cell&& c, const T& x) {
    c.ss_ << x;
    return std::move(c);
  }
  std::string str() const { return ss_.str(); }
};

/** Column delimiters of a table. */
struct ColSeparators {
  std::string left = "";
  std::string mid = " ";
  std::string right = "";
};

/** Base class for row delimiters (horizontal lines between rows). */
class RowSeparator {
 public:
  /// Print row separator to stream.
  /// @param  out          Output stream to print
  /// @param  spec_widths  User specified widths of each column
  /// @param  widths       Computed widths of each column (from content text)
  /// @param  aligns       User specified alignments of each column
  virtual void print(std::ostream& out,
                     const Widths& spec_widths,
                     const Widths& widths,
                     const Aligns& aligns) const = 0;

  virtual ~RowSeparator() {}
};

/** A row separator that does not align to columns (e.g. Latex's \hline). */
class RowSeparatorFlat : public RowSeparator {
 private:
  std::string sepr_;

 public:
  RowSeparatorFlat(std::string sepr = "") : sepr_(std::move(sepr)) {}

  void print(std::ostream& out,
             const Widths& /*spec_widths*/,
             const Widths& /*widths*/,
             const Aligns& /*aligns*/) const override {
    out << sepr_ << std::endl;
  }
};

/** Empty row separator, does not add any line between two consecutive rows. */
class RowSeparatorEmpty : public RowSeparator {
 public:
  void print(std::ostream& /*out*/,
             const Widths& /*spec_widths*/,
             const Widths& /*widths*/,
             const Aligns& /*aligns*/) const override {}
};

/// A row separator that has column separators within that aligns to each
/// cell/column (e.g. Markdown)
class RowSeparatorColwise : public RowSeparator {
 private:
  ColSeparators col_sepr_;
  std::string filler_;

 public:
  RowSeparatorColwise(ColSeparators csep = {}, std::string fill = " ")
      : col_sepr_(std::move(csep)), filler_(std::move(fill)) {
    assert(not filler_.empty());
  }

  void print(std::ostream& out,
             const Widths& spec_widths,
             const Widths& widths,
             const Aligns& /*aligns*/) const override {
    static auto extend = [](const std::string& s, const size_t width) {
      std::string rval;
      size_t lens = len(s);
      for (size_t _ = 0; _ < width / lens; _++) { rval += s; }
      rval += substr(s, 0, width % lens);
      return rval;
    };

    out << col_sepr_.left;
    for (size_t i = 0; i < widths.size(); i++) {
      if (i > 0) { out << col_sepr_.mid; }
      size_t width = (i < spec_widths.size() and spec_widths[i] > 0)
                         ? spec_widths[i]
                         : widths[i];
      out << extend(filler_, width);
    }
    out << col_sepr_.right << std::endl;
  }
};

/** Collection of row separators that belong to a single table. */
struct RowSeparators {
  using Separator = RowSeparatorEmpty;
  /* Vars: Row separator shared pointers

     top        - Separator at the top (border above whole table)
     header_mid - Separator below header
     mid        - Separator in between non-header rows
     bottom     - Separator below table (border below whole table)
  */
  /** Separator at the top (border above whole table) */
  std::shared_ptr<RowSeparator> top = std::make_shared<Separator>();
  std::shared_ptr<RowSeparator> header_mid = std::make_shared<Separator>();
  std::shared_ptr<RowSeparator> mid = std::make_shared<Separator>();
  std::shared_ptr<RowSeparator> bottom = std::make_shared<Separator>();
};

/// Combination of column and row separators that make up the layout of a
/// table.
struct Layout {
  ColSeparators col_sepr;
  RowSeparators row_sepr;
};

/** Main class that defines a table with its layout and content. */
class Table {
 public:
  /// A collection of rows of a table, which contain the text content.
  /// See Row.
  using Grid = std::vector<Row>;

 private:
  Grid data_;
  Row cur_row_;

  Widths spec_widths_; /**< Specified widths of each column */
  Aligns spec_aligns_; /**< Specified alignment of each column */
  LineSplitter split_ = Naive; /** Line splitting method */
  Layout layout_; /** Layout definition of the table, see <Layout>. */

  /// Widths of each column based only on its text content. Can be different
  /// from user specified widths (<spec_widths_>).
  Widths widths_;
  // bool printed_any_row_ = false; //TODO: to be used in online mode

  std::stringstream ss_;

  // Helpers

  /// Given a string, width and alignment, align and print the string. If string
  /// length is smaller than width, rest of width is filled by space.

  /// Preconditions:

  /// - Single line (does not have \n in it)
  /// - s.size() <= width
  static void aligned_print_(std::ostream& out,
                             const std::string& s,
                             size_t width,
                             Align align);
  /// Print a string in the given width and alignment, and
  /// return the remaining suffix string that did not fit in width.
  static std::string print_(std::ostream& out,
                            const std::string& s,
                            size_t width,
                            Align align,
                            LineSplitter ls);
  /// Print _single_ line of a row. A row can contain multiple lines if
  /// multiline is enabled.
  Row print_row_line_(std::ostream& out, const Row& row) const;

  /** Print a row. If the row has multiple lines, prints all of them. */
  void print_row_(std::ostream& out, const Row& row) const;

 public:
  /// Set widths of each column. Zero means auto. If there are more
  /// columns than widths, underspecified columns default to zero.
  Table& widths(Widths widths_) {
    spec_widths_ = std::move(widths_);
    return *this;
  }
  /// Set alignments of each column. If there are more columns than
  /// alignments, underspecified columns default to Left.
  Table& aligns(Aligns aligns_) {
    spec_aligns_ = std::move(aligns_);
    return *this;
  }
  /** Set multilineness (line splitter) of table. */
  Table& multiline(LineSplitter mline) {
    split_ = std::move(mline);
    return *this;
  }
  /** Set layout of table. */
  Table& layout(Layout layout) {
    layout_ = std::move(layout);
    return *this;
  }
  /** Set floating point precision. */
  Table& precision(const int n) {
    ss_ << std::setprecision(n);
    return *this;
  }
  /** Set fixed notation for printing floats (as opposed to default). */
  Table& fixed() {
    ss_ << std::fixed;
    return *this;
  }

  /// Stream operator to pipe things _into_ the table.
  /// General (unspecialized) version assumes input is streamable to a stream
  /// and puts it into the next cell by converting it to string.
  template <typename T>
  Table& operator<<(const T& x);

  /// Print the table. This is the final method to call after table contents are
  /// populated and ready to be displayed / output.
  void print(std::ostream& out = std::cout) const;
};

template <typename T>
Table& Table::operator<<(const T& x) {
  // insert the value into the table as a string
  ss_ << x;
  ss_.str(normalize_newlines(ss_.str()));
  cur_row_.push_back(ss_.str());

  widths_.resize(std::max(widths_.size(), cur_row_.size()), 0);
  size_t& width = widths_[cur_row_.size() - 1];
  for (std::string s; std::getline(ss_, s); width = std::max(width, len(s))) {}

  ss_.str("");
  ss_.clear();

  return *this;
}

/// Specialization of the stream operator for Endr type to end a row.
///
/// Example:
///
/// ~~~~{.cpp}
/// t << "hello" << "world" << tblr::endr;
/// ~~~~
template <>
inline Table& Table::operator<<(const Endr&) {
  data_.push_back(std::move(cur_row_));
  return *this;
}

/// Specialization of the stream operator for <Cells>. Puts the cell contents
/// into the next cell.
template <>
inline Table& Table::operator<<(const Cell& c) {
  return *this << normalize_newlines(c.str());
}

inline void Table::aligned_print_(std::ostream& out,
                                  const std::string& s,
                                  size_t width,
                                  Align align) {
  size_t lens = len(s);
  assert(lens <= width and
         s.find('\n') == std::string::npos); // paranoid ¯\_(ツ)_/¯

  if (align == Left) {
    out << s << std::string(width - lens, ' ');
  } else if (align == Center) {
    out << std::string((width - lens) / 2, ' ') << s
        << std::string((width - lens + 1) / 2, ' ');
  } else if (align == Right) {
    out << std::string(width - lens, ' ') << s;
  }
}

inline std::string Table::print_(std::ostream& out,
                                 const std::string& s,
                                 size_t width,
                                 Align align,
                                 LineSplitter ls) {
  std::string head = s;
  std::string tail = "";

  // split by '\n'
  size_t pos = s.find('\n');
  if (pos != std::string::npos) {
    head = s.substr(0, pos);
    tail = s.substr(pos + 1);
  }

  // split by width
  if (len(head) > width) {
    head = substr(s, 0, width);
    tail = substr(s, width);
    if (ls == Space) {
      // split by space
      pos = head.rfind(' ');
      if (pos != std::string::npos) {
        head = s.substr(0, pos);
        tail = s.substr(pos + 1);
      }
    }
  }

  aligned_print_(out, head, width, align);
  return (ls == SingleLine) ? "" : tail;
}

inline Row Table::print_row_line_(std::ostream& out, const Row& row) const {
  Row rval;
  out << layout_.col_sepr.left;
  for (size_t i = 0; i < row.size(); i++) {
    if (i > 0) { out << layout_.col_sepr.mid; }
    size_t width = (i < spec_widths_.size() and spec_widths_[i] > 0)
                       ? spec_widths_[i]
                       : widths_[i];
    Align align = (i < spec_aligns_.size()) ? spec_aligns_[i] : Left;
    rval.push_back(print_(out, row[i], width, align, split_));
  }
  out << layout_.col_sepr.right << std::endl;
  return rval;
}

inline void Table::print_row_(std::ostream& out, const Row& row) const {
  static auto empty = [](const Row& row) {
    return std::all_of(
        row.begin(), row.end(), std::mem_fn(&std::string::empty));
  };

  Row rval = row;
  while (not empty(rval = print_row_line_(out, rval))) {}
}

inline void Table::print(std::ostream& out) const {
  auto& row_sepr = layout_.row_sepr;
  row_sepr.top->print(out, spec_widths_, widths_, spec_aligns_);
  for (size_t i = 0; i < data_.size(); i++) {
    if (i == 1) {
      row_sepr.header_mid->print(out, spec_widths_, widths_, spec_aligns_);
    } else if (i > 1) {
      row_sepr.mid->print(out, spec_widths_, widths_, spec_aligns_);
    }
    print_row_(out, data_[i]);
  }
  row_sepr.bottom->print(out, spec_widths_, widths_, spec_aligns_);
}

/// Stream operator to stream a table to an output stream. Alternative to
/// calling <Table::print> explicitly.
inline std::ostream& operator<<(std::ostream& os, const Table& t) {
  t.print(os);
  return os;
}

// Predefined Layouts

/// Creates a simple layout with specified border elements. Row separators align
/// to columns.
inline Layout simple_border(std::string left,
                            std::string center,
                            std::string right,
                            std::string top,
                            std::string header_mid,
                            std::string mid,
                            std::string bottom) {
  ColSeparators cs{std::move(left), std::move(center), std::move(right)};
  RowSeparators rs{
      std::make_shared<RowSeparatorColwise>(cs, std::move(top)),
      std::make_shared<RowSeparatorColwise>(cs, std::move(header_mid)),
      std::make_shared<RowSeparatorColwise>(cs, std::move(mid)),
      std::make_shared<RowSeparatorColwise>(cs, std::move(bottom))};
  return {std::move(cs), std::move(rs)};
}

/// Creates a simple layout with specified column separators and only
/// a single row separator between the header
/// and the rest of the table. Separator aligns to columns.
inline Layout simple_border(std::string left,
                            std::string center,
                            std::string right,
                            std::string header_mid) {
  ColSeparators cs{std::move(left), std::move(center), std::move(right)};
  RowSeparators rs{
      std::make_shared<RowSeparatorEmpty>(),
      std::make_shared<RowSeparatorColwise>(cs, std::move(header_mid)),
      std::make_shared<RowSeparatorEmpty>(),
      std::make_shared<RowSeparatorEmpty>()};
  return {std::move(cs), std::move(rs)};
}

/// Creates a simple layout with specified column separators and no row
/// separators.
inline Layout
simple_border(std::string left, std::string center, std::string right) {
  ColSeparators cs{std::move(left), std::move(center), std::move(right)};
  RowSeparators rs{std::make_shared<RowSeparatorEmpty>(),
                   std::make_shared<RowSeparatorEmpty>(),
                   std::make_shared<RowSeparatorEmpty>(),
                   std::make_shared<RowSeparatorEmpty>()};
  return {std::move(cs), std::move(rs)};
}

/// Creates layout with markdown table syntax. Resulting table can be copy pasted
/// into markdown. Markdown specific alignment syntax is not yet supported.
inline Layout markdown() {
  return simple_border("| ", " | ", " |", "-");
}

/** Creates a very basic indented list layout with whitespace delimiters. */
inline Layout indented_list() {
  return simple_border("  ", "   ", "");
}

class LatexHeader : public RowSeparator {
 public:
  void print(std::ostream& out,
             const Widths& /*spec_widths*/,
             const Widths& /*widths*/,
             const Aligns& aligns) const override {
    out << R"(\begin{tabular}{)";
    for (auto& a : aligns) { out << (char)a; }
    out << "}" << std::endl << R"(\hline)" << std::endl;
  }
};

/// Creates a layout that matches latex tabular syntax. Resulting table can be
/// copy pasted into latex code within a table field. Alignments are also
/// converted to latex alignment syntax as part of the tabular command.
inline Layout latex() {
  ColSeparators cs{"", " & ", " \\\\"};
  RowSeparators rs{
      std::make_shared<LatexHeader>(),
      std::make_shared<RowSeparatorFlat>("\\hline"),
      std::make_shared<RowSeparatorEmpty>(),
      std::make_shared<RowSeparatorFlat>("\\hline\n\\end{tabular}")};
  return {std::move(cs), std::move(rs)};
}

/** Creates a spacious layout. */
inline Layout extra_space() {
  return simple_border("  ", "  ", "  ", " ", " ", " ", " ");
}

/** Creates a box layout with ascii edges and corners */
inline Layout ascii_box() {
  ColSeparators sep{"+", "-", "+"};
  RowSeparators rs{std::make_shared<RowSeparatorColwise>(sep, "-"),
                   std::make_shared<RowSeparatorColwise>(sep, "-"),
                   std::make_shared<RowSeparatorColwise>(sep, "-"),
                   std::make_shared<RowSeparatorColwise>(std::move(sep), "-")};

  return {ColSeparators{"|", "|", "|"}, rs};
}

/** Creates a box layout with light unicode borders */
inline Layout unicode_box_light() {
  ColSeparators top{"┌", "┬", "┐"};
  ColSeparators mid{"├", "┼", "┤"};
  ColSeparators bot{"└", "┴", "┘"};
  RowSeparators rs{std::make_shared<RowSeparatorColwise>(std::move(top), "─"),
                   std::make_shared<RowSeparatorColwise>(mid, "─"),
                   std::make_shared<RowSeparatorColwise>(std::move(mid), "─"),
                   std::make_shared<RowSeparatorColwise>(std::move(bot), "─")};

  return {ColSeparators{"│", "│", "│"}, rs};
}

} // namespace tblr

#endif