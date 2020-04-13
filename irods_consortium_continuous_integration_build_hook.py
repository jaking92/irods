from __future__ import print_function

import glob
import itertools
import logging
import multiprocessing
import optparse
import os
import sys
import tempfile
import time

import irods_python_ci_utilities

def install_building_dependencies(externals_directory):
    externals_list = [
        'irods-externals-cmake3.11.4-0',
        'irods-externals-avro1.9.0-0',
        'irods-externals-boost1.67.0-0',
        'irods-externals-catch22.3.0-0',
        'irods-externals-clang-runtime6.0-0',
        'irods-externals-clang6.0-0',
        'irods-externals-cppzmq4.2.3-0',
        'irods-externals-fmt6.1.2-1',
        'irods-externals-json3.7.3-0',
        'irods-externals-libarchive3.3.2-1',
        'irods-externals-nanodbc2.13.0-0',
        'irods-externals-spdlog1.5.0-1',
        'irods-externals-zeromq4-14.1.6-0'
        ]
    if externals_directory is None or externals_directory == 'None':
        irods_python_ci_utilities.install_irods_core_dev_repository()
        irods_python_ci_utilities.install_os_packages(externals_list)
    else:
        package_suffix = irods_python_ci_utilities.get_package_suffix()
        os_specific_directory = irods_python_ci_utilities.append_os_specific_directory(externals_directory)
        externals = []
        for irods_externals in externals_list:
            local_package_path = os.path.join(os_specific_directory, irods_externals + '*.{0}'.format(package_suffix))
            externals.append(glob.glob(local_package_path)[0])
        irods_python_ci_utilities.install_os_packages_from_files(externals)

    add_cmake_to_front_of_path()
    install_os_specific_dependencies()

def add_cmake_to_front_of_path():
    cmake_path = '/opt/irods-externals/cmake3.11.4-0/bin'
    os.environ['PATH'] = os.pathsep.join([cmake_path, os.environ['PATH']])

def install_os_specific_dependencies():
    dispatch_map = {
        'Ubuntu': install_os_specific_dependencies_apt,
        'Centos': install_os_specific_dependencies_yum,
        'Centos linux': install_os_specific_dependencies_yum,
    }
    try:
        return dispatch_map[irods_python_ci_utilities.get_distribution()]()
    except KeyError:
        irods_python_ci_utilities.raise_not_implemented_for_distribution()

def install_os_specific_dependencies_apt():
    if irods_python_ci_utilities.get_distribution() == 'Ubuntu': # cmake from externals requires newer libstdc++ on ub12
        if irods_python_ci_utilities.get_distribution_version_major() == '12':
            irods_python_ci_utilities.install_os_packages(['python-software-properties'])
            irods_python_ci_utilities.subprocess_get_output(['sudo', 'add-apt-repository', '-y', 'ppa:ubuntu-toolchain-r/test'], check_rc=True)
            irods_python_ci_utilities.install_os_packages(['libstdc++6'])

    irods_python_ci_utilities.install_os_packages([
        'fakeroot', 'help2man', 'libbz2-dev', 'libcurl4-gnutls-dev', 'libkrb5-dev', 'libpam0g-dev',
        'libssl-dev', 'make', 'python-dev', 'unixodbc', 'unixodbc-dev', 'zlib1g-dev',
    ])

def install_os_specific_dependencies_yum():
    packages_to_install = [
        'bzip2-devel', 'curl-devel', 'fakeroot', 'help2man', 'openssl-devel',
        'pam-devel', 'python-devel', 'unixODBC', 'unixODBC-devel', 'zlib-devel',
    ]
    if irods_python_ci_utilities.get_distribution_version_major() == '7':
        packages_to_install.append('mysql++-devel')
    irods_python_ci_utilities.install_os_packages(packages_to_install)

def build_irods(debug_build):
    irods_source_dir = os.path.dirname(os.path.realpath(__file__))
    irods_build_dir = tempfile.mkdtemp(prefix='irods_build_dir')
    logging.getLogger(__name__).info('Using iRODS build directory: %s', irods_build_dir)
    if debug_build:
        cmake_build_type = 'Debug'
    else:
        cmake_build_type = 'Release'
    irods_python_ci_utilities.subprocess_get_output('cmake {0} -DCMAKE_BUILD_TYPE={1} > cmake.output'.format(irods_source_dir, cmake_build_type), shell=True, cwd=irods_build_dir, check_rc=True)
    irods_python_ci_utilities.subprocess_get_output('make -j{0} > make.output'.format(str(multiprocessing.cpu_count())), shell=True, cwd=irods_build_dir, check_rc=True)
    irods_python_ci_utilities.subprocess_get_output('fakeroot make package >> make.output', shell=True, cwd=irods_build_dir, check_rc=True)
    return irods_build_dir

def copy_output_packages(irods_build_dir, output_root_directory):
    # Packages
    irods_python_ci_utilities.gather_files_satisfying_predicate(
        irods_build_dir,
        irods_python_ci_utilities.append_os_specific_directory(output_root_directory),
        lambda s: s.endswith(irods_python_ci_utilities.get_package_suffix()))
    # Unit-test binaries
    irods_python_ci_utilities.gather_files_satisfying_predicate(
        os.path.join(irods_build_dir, 'unit_tests'),
        irods_python_ci_utilities.append_os_specific_directory(output_root_directory),
        lambda s: os.path.basename(s).startswith('irods_'))

def register_log_handler():
    logging.getLogger().setLevel(logging.INFO)
    logging_handler = logging.StreamHandler(sys.stdout)
    logging_handler.setFormatter(logging.Formatter(
       '%(asctime)s - %(levelname)7s - %(filename)30s:%(lineno)4d - %(message)s',
       '%Y-%m-%dT%H:%M:%SZ'))
    logging_handler.formatter.converter = time.gmtime
    logging_handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(logging_handler)

def main():
    parser = optparse.OptionParser()
    parser.add_option('--debug_build', action='store_true', default=False)
    parser.add_option('--externals_packages_directory')
    parser.add_option('--output_root_directory')
    parser.add_option('--just_install_dependencies', action='store_true', default=False)
    parser.add_option('--verbose', action='store_true', default=False)
    options, _ = parser.parse_args()

    if options.verbose:
        register_log_handler()

    install_building_dependencies(options.externals_packages_directory)

    if options.just_install_dependencies:
        return

    irods_build_dir = build_irods(options.debug_build)

    if options.output_root_directory:
        copy_output_packages(irods_build_dir, options.output_root_directory)

if __name__ == '__main__':
    main()
