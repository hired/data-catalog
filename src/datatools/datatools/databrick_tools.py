import requirements

def requirements_file_to_dependency_list(requirements_filename='requirements.txt'):
    fh = open(requirements_filename)
    dependency_libraries = []
    for req in requirements.parse(fh):
        if req.editable:
            raise NameError('Editable packages not supported yet "%s"' % req.line)
        dependency_libraries.append({"pypi": {"package": req.name}})

    fh.close()
    return dependency_libraries
