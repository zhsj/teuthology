import os

class TeuthologyReport:
    def __init__(self, title):
        self.base_section = ReportSection(0)
        self.base_section.header.append(title)

    def __str__(self):
        return (str) (self.base_section)

    def add_section(self, title):
        return self.base_section.add_section(title)

class ReportSection:
    def __init__(self, depth):
        self.lines = []
        self.header = []
        self.sections = []
        self.depth = depth

    def __str__(self):
        out = os.linesep.join(self.make_header())
        if self.lines: 
            out = out + os.linesep.join(self.make_lines())
        if self.sections:
            for section in self.sections[:-1]:
                out = out + os.linesep + str(section) + os.linesep
            out = out + os.linesep + str(self.sections[-1])
        return out

    def add_section(self, title):
        section = ReportSection(self.depth + 1)
        section.header.append(title)
        self.sections.append(section)
        return section

    def make_header(self):
        formatted = []
        indent = self.depth 
        formatted.append('  ' * indent + '#'.ljust(78 - 2 * indent, '#'))
        for line in self.header:
            line = ('  ' * indent + '# ' + line).ljust(77) + '#'
            formatted.append(line)
        formatted.append('  ' * indent + '#'.ljust(78 - 2 * indent, '#'))
#        formatted.append('#' * 78)
        formatted.append('')
        return formatted

    def make_lines(self):
        indent = self.depth 
        formatted = ['']
        for line in self.lines:
            formatted.append('  ' * indent + line)
#        formatted.append('')
        return formatted

if __name__ == '__main__':
    report = TeuthologyReport('test')
    section1 = report.add_section('test section 1')
    section1.lines.append('test section 1 text')
    section2 = report.add_section('test section 2')
    section2.lines.append('test section 2 text')
    section2 = report.add_section('test section 2')
    section2.lines.append('test section 2 text')
    section3 = report.add_section('test section 3')
    section3.lines.append('test section 3 text')

    print report

