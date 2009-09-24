#!/usr/bin/perl -w
#
#  (C) 2009 by Argonne National Laboratory.
#      See COPYRIGHT in top-level directory.
#

# takes a standard mpicc script as an argument and tried to generate a
# darshan-enabled mpicc script to mimic it's behavior

use Getopt::Long;
use English;

my $input_file = "";
my $output_file = "";

process_args();

# run original mpicc with -show argument to capture command line for
# compilation and linking
my $compile_cmd = `$input_file -show -c foo.c`;
if (!($compile_cmd))
{
    printf STDERR "Error: failed to invoke $input_file with -show\n";
    exit(1);
}

my $link_cmd = `$input_file -show foo.o -o foo`;
if (!($link_cmd))
{
    printf STDERR "Error: failed to invoke $input_file with -show\n";
    exit(1);
}

#print "compile: $compile_cmd\n";
#print "link: $link_cmd\n";

# check command lines for accuracy
if(!($compile_cmd =~ /-c foo.c/) || !($link_cmd =~ /foo.o -o foo/))
{
    printf STDERR "Error: faulty output from $input_file with -show.\n";
    exit(1);
}

open (OUTPUT, ">$output_file") || die("Error opening output file: $!");

print OUTPUT <<'EOF';
#!/bin/sh
#
# Auto-generated mpicc script from darshan-gen-cc.pl
#
#
# Internal variables
# Show is set to echo to cause the compilation command to be echoed instead 
# of executed.
Show=

linking=yes
allargs=("$@")
argno=0
for arg in "$@" ; do
    # Set addarg to no if this arg should be ignored by the C compiler
    addarg=yes
    case "$arg" in 
 	# ----------------------------------------------------------------
	# Compiler options that affect whether we are linking or no
    -c|-S|-E|-M|-MM)
    # The compiler links by default
    linking=no
    ;;
    -show)
    addarg=no
    Show=echo
    ;;
    esac
    if [ $addarg = no ] ; then
	unset allargs[$argno]
    fi
    # Some versions of bash do not accept ((argno++))
    argno=`expr $argno + 1`
done
if [ "$linking" = yes ] ; then
    if [ -n "$C_LINKPATH_SHL" ] ; then
	# prepend the path for the shared libraries to the library list
	mpilibs="$C_LINKPATH_SHL$libdir $mpilibs"
    fi
    $Show $CC "${allargs[@]}" -I$includedir $CFLAGS $LDFLAGS -L$libdir $mpilibs $MPI_OTHERLIBS
    rc=$?
else
    $Show $CC "${allargs[@]}" -I$includedir $CFLAGS
    rc=$?
fi

exit $rc
EOF

close(OUTPUT);

chmod(0755, $output_file); 

exit(0);

sub process_args
{
    use vars qw( $opt_help $opt_output );

    Getopt::Long::Configure("no_ignore_case", "bundling");
    GetOptions( "help",
        "output=s");

    if($opt_help)
    {
        print_help();
        exit(0);
    }

    if($opt_output)
    {
        $output_file = $opt_output;
    }
    else
    {
        print_help();
        exit(1);
    }

    # there should only be one remaining argument: the input file 
    if($#ARGV != 0)
    {
        print "Error: invalid arguments.\n";
        print_help();
        exit(1);
    }
    $input_file = $ARGV[0];

    return;
}

sub print_help
{
    print <<EOF;

Usage: $PROGRAM_NAME <stock mpicc> --output <modified mpicc>

    --help          Prints this help message
    --output        Specifies name of output script

Purpose:

    This script takes an existing mpicc script as input and generates a
    modified version that includes Darshan support.

EOF
    return;
}

