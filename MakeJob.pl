#!/usr/bin/perl

use strict;
use warnings;
use lib '/star/u/dkap7827/Tools2/Tools/PerlScripts';
use CondorJobWriter;
use Getopt::Long qw(GetOptions);
use Getopt::Long qw(HelpMessage);
use Pod::Usage;

=pod

=head1 NAME

MakeJob - Handy script for submitting condor jobs for FCS analysis and checking job output

=head1 SYNOPSIS

MakeJob.pl [option] [value] ...

  --ana, -a     Directory where analysis code is (i.e. run macro, star libraries) (default is current directory)
  --data, -d    (required) Directory where data is located or file that contains file locations for the data (i.e. daq files, mudst files)
  --out, -o     Directory where to create files for the job, creates a seperate ID for each job so only need a generic location (default is current directory)
  --check, -c   Check if all files in the summary match the ones in the output folder
  --mode, -m    Kind of job to submit:daq, mudst (default is mudst)
  --test, -t    Test only, creates a test directory and only processes 5 data files
  --verbose, -v level set the printout level (default is 1)
  --help, -h    print this help

=head1 VERSION

0.1

=head1 DESCRIPTION

This program will create job files for submitting FCS jobs. It will then use "CondorJobWriter" to generate folders and files for submitting the executable for the Fms Qa to condor batch system to be executed.

=cut

=begin comment
Author: David Kapukchyan

@[December 1,2022]
> First instance
=cut

#Here is the setup to check if the submit option was given
my $LOC = $ENV{'PWD'};
my $ANADIR = $LOC;
my $DATA = "";
my @DATAFILES;
#$DATADIR = "/gpfs01/star/pwg_tasks/FwdCalib/DAQ/23080044";
my $OUTDIR = $LOC;
my $CHECKDIR = "";
my $DOMISSING = 0;
my $VERBOSE = 1;
my $TEST;
my $MODE = "mudst";
#my $DEBUG;

GetOptions(
    'ana|a=s'     => \$ANADIR,
    'data|d=s'    => \$DATA,
    'out|o=s'     => \$OUTDIR,
    'check|c=s'   => \$CHECKDIR,
    'missing|r=i' => \$DOMISSING,
    'mode|m=s'    => \$MODE,
    'test|t'      => \$TEST,
    'verbose|v=i' => \$VERBOSE,
    'help|h'      => sub { HelpMessage(0) }
    ) or HelpMessage(1);


if( $CHECKDIR ){
    my $char = chop $CHECKDIR;
    while( $char eq "/" ){$char = chop $CHECKDIR;}
    $CHECKDIR = $CHECKDIR.$char;
}

if( $CHECKDIR && !$DOMISSING ){
    CompareOutput($CHECKDIR,2); #Force print
    exit(0);
}

if( $DOMISSING ){
    if( ! $CHECKDIR ){ print "ERROR:Please provide directory using option 'c'"; HelpMessage(0); }
    
    my %MissingJobs = CompareOutput($CHECKDIR,$VERBOSE);   #Print only when verbose>=2

    my $hash = substr($CHECKDIR,-7);

    open( my $oldcondor_fh, '<', "$CHECKDIR/condor/condor_$hash.job" ) or die "Could not open file '$CHECKDIR/condor/condor_$hash.job' for reading: $!";
    if( -f "$CHECKDIR/condor/condor${DOMISSING}_$hash.job" ){
	print "WARNING:condor${DOMISSING}_$hash.job exists!\nOverwrite(Y/n): ";
	my $input = <STDIN>; chomp $input;
	if( $input ne "Y" ){ die "Quitting to prevent overwrite of condor${DOMISSING}_$hash.job\n"; }
    }
    open( my $newcondor_fh, '>', "$CHECKDIR/condor/condor${DOMISSING}_$hash.job" ) or die "Could not open file '$CHECKDIR/condor/condor${DOMISSING}_$hash.job' for writing: $!";

    while( my $oldline = <$oldcondor_fh> ){
	if( $oldline =~ /##### \d*/ ){
	    my $jobnumber = $oldline;
	    $jobnumber =~ s/##### //;
	    $jobnumber += 0;
	    #chomp $oldline; print "$oldline | $jobnumber\n";
	    if( $MissingJobs{$jobnumber} ){
		print $newcondor_fh $oldline;
		while( $oldline = <$oldcondor_fh> ){
		    print $newcondor_fh $oldline;
		    if( $oldline eq "\n" ){ last; } #The condor job file always has a new line separting different job indexes
		}
	    }
	}
    }
    
    exit(0);
}
    

if( !(-d "$ANADIR") ){ HelpMessage(0); }#die "Directory $ANADIR is not a directory or does not exist: $!"; }
else{
    my $char = chop $ANADIR; #Get last character of ANA
    while( $char eq "/" ){$char = chop $ANADIR;} #Remove all '/'
    $ANADIR = $ANADIR.$char; #Append removed character which was not a '/'
}
if( $VERBOSE>=1 ){ print "ANADIR=$ANADIR\n"; }
if( $VERBOSE>=2 ){ print "Contents ANADIR\n"; `ls $ANADIR`; }

my $CshellMacro = "";
if(    $MODE eq "daq"   ){ $CshellMacro = "RunBfc.csh";   }
elsif( $MODE eq "mudst" ){ $CshellMacro = "RunMuDst.csh"; }
else{ print "Invalid Mode: $MODE\n"; HelpMessage(0); }

if( !(-e "$DATA") ){ print "ERROR:'$DATA' does not exist: $!\n";  HelpMessage(0); }
else{
    if( -f "$DATA" ){
	open( my $data_fh, '<', $DATA ) or die "Could not open file '$DATA' for reading: $!";
	while( my $line = <$data_fh> ){
	    chomp $line;
	    push @DATAFILES, $line;
	    if( $VERBOSE>=3 ){ print "$line\n"; }
	}
    }
    elsif( -d "$DATA" ){
	my $char = chop $DATA; #Get last character of DATA
	while( $char eq "/" ){$char = chop $DATA;} #Remove all '/'
	$DATA = $DATA.$char; #Append removed character which was not a '/'
	opendir my $dh, $DATA or die "Could not open '$DATA' for reading '$!'\n";
	while( my $datafile = readdir $dh ){
	    if( $VERBOSE>=3 ){ print "$datafile\n"; }
	    if( $MODE eq "daq" ){
		if( $datafile =~ m/st_fwd_\d{8}_\w*.daq/ ){ push @DATAFILES, "$DATA/$datafile"; }
		if( $VERBOSE>=2 ){ print " - $datafile\n"; }
	    }
	    if( $MODE eq "mudst" ){
		if( $datafile =~ m/st_fwd_\d{8}_\w*.MuDst.root/ ){ push @DATAFILES, "$DATA/$datafile"; }
		if( $VERBOSE>=2 ){ print " - $datafile\n"; }
	    }
	}
	closedir $dh;
    }
    else{ print "ERROR:'$DATA' is not a directory or file:$!\n";  HelpMessage(0); }
}
if( $VERBOSE>=1 ){ print "DATA=$DATA\n"; }

if( !(-d "$OUTDIR") ){
    system("/bin/mkdir $OUTDIR") == 0 or die "Unable to make '$OUTDIR': $!";

}
my $char = chop $OUTDIR; #Get last character of OUTDIR
while( $char eq "/" ){$char = chop $OUTDIR;} #Remove all '/'
$OUTDIR = $OUTDIR.$char; #Append removed character which was not a '/'
if( $VERBOSE>=1 ){ print "OUTDIR=$OUTDIR\n"; }
if( $VERBOSE>=2 ){ print "Contents OUTDIR\n"; `ls $OUTDIR`; }

#Get Time
my $epochtime = time();            #UNIX time (seconds from Jan 1, 1970)
my $localtime = localtime();       #Human readaable time

my $UUID = $TEST ? "TEST\n" : uc(`uuidgen`);#Command that generates a UUID in bash
chomp($UUID);
$UUID =~ s/-//g;
if( $VERBOSE>=1 ){print "Job Id: $UUID\n"};

my $UUID_short = substr($UUID,0,7); #Shortened 7 character UUID for file location.
if( $VERBOSE>=2 ){ print "Shortened UUID: $UUID_short\n"; }
my $FileLoc = "$OUTDIR/$UUID_short";  #Main location for files

if (! -e "$FileLoc") {system("/bin/mkdir $FileLoc") == 0 or die "Unable to make '$FileLoc': $!";}
elsif( $TEST ){
    print "Remove all files in test folder $FileLoc (Y/n):";
    my $input = <STDIN>; chomp $input;
    if( $input eq "Y" ){system("/bin/rm -r $FileLoc/*") == 0 or die "Unable to remove files in '$FileLoc': $!";}
}
else{ die "ERROR:Matched shortened UUID: ${UUID_short}\n" }

if( $VERBOSE>=1 ){print "All Files to be written in '$FileLoc'\n";}

my $JobWriter = new CondorJobWriter($FileLoc,"${CshellMacro}","","${UUID_short}");  #Writes the condor job files
#Need to create directory here since this is where executable gets installed
my $CondorDir = $JobWriter->CheckDir("condor", $TEST);  #if it doesn't exist: create condor directory, if it does exist:prompt for removal if not testing
#Because of the way condor job submission works the executable and the job file must be in the same directory, which is why most everything is set with respect to the condor directory

my $FileSummary = "$FileLoc/Summary_${UUID}.list";  #This file will describe the kind of job that was submitted and what the data it will contain
open( my $fh_sum, '>', $FileSummary ) or die "Could not open file '$FileSummary' for writing: $!";

print $fh_sum "UUID: $UUID\n";               #print UUID for job
print $fh_sum "Epoch Time: $epochtime\n";    #print UNIX time
print $fh_sum "Time: $localtime\n";          #print local time
print $fh_sum "Main directory: $LOC\n";      #print directory job was created on
print $fh_sum "Ana: $ANADIR\n";              #print analysis directory
print $fh_sum "Data: $DATA\n";               #print data directory/file
print $fh_sum "Out: $OUTDIR\n";              #print directory where the folder with the job UUID will go
print $fh_sum "Mode: $MODE\n";               #Print Mode (daq,mudst,sim)
print $fh_sum "Macro: ${CshellMacro}\n";     #print Macro
print $fh_sum "Verbose: $VERBOSE\n";         #print verbose option
print $fh_sum "Node: $ENV{HOST}\n";          #print node job was submitted on

print "Making ${CshellMacro}\n";
if( $MODE eq "daq" ){
    WriteBfcShellMacro( "${CondorDir}/${CshellMacro}", "${CondorDir}" );
}
if( $MODE eq "mudst" ){
    WriteMuDstShellMacro( "${CondorDir}/${CshellMacro}", "${CondorDir}" );
    system("/bin/cp $ANADIR/runMudst.C $CondorDir") == 0 or die "Unable to copy 'runMudst.C': $!";
    $JobWriter->AddInputFiles("$CondorDir/runMudst.C");
   # system("/bin/cp $ANADIR/fcsgaincorr.txt $CondorDir") == 0 or die "Unable to copy 'fcsgain.txt': $!";
   # $JobWriter->AddInputFiles("$CondorDir/fcsgaincorr.txt");
    my $starlibloc = "." . $ENV{'STAR_HOST_SYS'};
    system("/bin/cp -L -r $ANADIR/$starlibloc $CondorDir") == 0 or die "Unable to copy '$starlibloc': $!";#-L to follow symlinks
    $JobWriter->AddInputFiles("$CondorDir/$starlibloc");
}

#File paths need to relative to 'InitialDir'
#$JobWriter->AddInputFiles("condor/RunLibMudst.C,condor/libRunMudst.so");
#$JobWriter->AddInputFiles("/star/data03/daq/2022/040/23040001/st_fwd_23040001_raw_0000003.daq");

if( $VERBOSE>=1 ){
    print "Job Summary\n";
    if( $VERBOSE>=2 ){ print  "- Verbose: $VERBOSE\n"; }
    print "- UUID: ${UUID}\n";
    if( $VERBOSE>=2 ){ print "- ShortID: $UUID_short\n"; }
    if( $VERBOSE>=2 ){ print "- Submit Dir: $LOC\n"; }
    print "- Analysis Dir: $ANADIR\n";
    print "- Data: $DATA\n";
    print "- Output Dir: $OUTDIR\n";
    if( $VERBOSE>=2 ){
	print "  - Main Dir: ${FileLoc}\n";
        print "  - Condor Dir: ${CondorDir}\n";
    }
    if( $VERBOSE>=2 ){print "- Mode: $MODE\n"; }
    print "- Condor Macro: ${CshellMacro}\n";
    print "- Date: $localtime\n";
    if( $VERBOSE>=2 ) {print  "- Epoch Time: $epochtime\n"; }
    print "- Node: $ENV{HOST}\n";
}

if( $VERBOSE>=1 ){print "Making job file\n";}

my $numfiles = 0;
my $nevents = $TEST ? 100 : 1000000;
foreach my $datafile (@DATAFILES){
    if( $numfiles==5 && $TEST ){last;}
    #$JobWriter->SetArguments("100 st_fwd_23040001_raw_0000003.daq" );
    #$JobWriter->SetArguments("10000 st_fwd_23080044_raw_1000002.daq" );
    $JobWriter->SetArguments("$nevents $datafile" );
    $JobWriter->WriteJob($numfiles,$numfiles); #Ensures it will check directory existence only once
    print $fh_sum "$datafile\n";
    if( $VERBOSE>=2 ){ print "datafile\n"; }
    $numfiles++;
}

#$JobWriter->SubmitJob();
print $fh_sum "Total files: $numfiles\n";
close $fh_sum;

print "Total files: $numfiles\n";
print "Short ID: ${UUID_short}\n";

sub WriteBfcShellMacro
{
    my( $FullFileName, $AnaDir ) = @_;
    open( my $fh, '>', $FullFileName ) or die "Could not open file '$FullFileName' for writing: $!";
    my $macro_text = <<"EOF";
\#!/bin/csh

stardev
echo \$STAR_LEVEL
#Getting rid of 'cd' Output file will no longer bin condor dir but where it is supposed to go
#cd $AnaDir
\#\$1=number of events
\#\$2=inputfile

echo "NumEvents:\${1}\\ninputfile:\${2}"
#Files should be copied to temp directory in /home/tmp/dkap7827 or \$SCRATCH. Since each node has its own temporary disk space, a folder with my username directory may not exist in \$SCRATCH or /home/tmp/dkap7827

set tempdir = "/home/tmp/dkap7827"
if( ! -d \$tempdir ) then
    mkdir -p \$tempdir
endif

set name = `echo \$2 | awk '{n=split ( \$0,a,"/" ) ; print a[n]}'`
echo \$name
if( ! -f \$tempdir/\$name ) then
    echo "Copying file"
    cp -v \$2 \$tempdir/\$name
endif
ls \$tempdir

if( -f \$tempdir/\$name ) then
    ln -s \$tempdir/\$name
    ls -a \$PWD
    echo "root4star -b -q bfc.C'(\$1,"\\"DbV20221012,pp2022,-picowrite,-hitfilt,-evout\\"","\\"\$name\\"")'"
    root4star -b -q bfc.C'('\$1',"DbV20221012,pp2022,-picowrite,-hitfilt,-evout","'\$name'")'
    #Remove copied file since temp disks vary from node to node
    rm \$name
    rm \$tempdir/\$name
else
    echo "ERROR:copy failed or file '\$tempdir/\$name' does not exist!"
endif
EOF

    print $fh $macro_text;
    close $fh;
    #Need to give execute permissions otherwise condor won't be able to run it
    system( "/usr/bin/chmod 755 $FullFileName" )==0 or die "Unable to give execute permisions to CshellMacro: $!\n";
}

sub WriteMuDstShellMacro
{
    my( $FullFileName, $AnaDir ) = @_;
    print "$FullFileName\n";
    print "$AnaDir\n";
    open( my $fh, '>', $FullFileName ) or die "Could not open file '$FullFileName' for writing: $!";
    my $macro_text = <<"EOF";
\#!/bin/csh

stardev
echo \$STAR_LEVEL
#Getting rid of 'cd' Output file will no longer bin condor dir but where it is supposed to go
#cd $AnaDir
\#\$1=number of events
\#\$2=inputfile

echo "NumEvents:\${1}\\ninputfile:\${2}"
#Files should be copied to temp directory in /home/tmp/dkap7827 or \$SCRATCH. Since each node has its own temporary disk space, a folder with my username directory may not exist in \$SCRATCH or /home/tmp/dkap7827

set tempdir = "/home/tmp/dkap7827"
if( ! -d \$tempdir ) then
    mkdir -p \$tempdir
endif

set name = `echo \$2 | awk '{n=split ( \$0,a,"/" ) ; print a[n]}'`
echo \$name
if( ! -f \$tempdir/\$2 ) then
    echo "Copying file"
    cp -v \$2 \$tempdir/\$name
endif
ls \$tempdir

if( -f \$tempdir/\$name ) then
    ln -s \$tempdir/\$name
    ls -a \$PWD
    echo "root4star -b -q runMuDst.C'("\\"\$name\\"",-1,\$1)'"
    root4star -b -q runMudst.C'('\\"\$name\\"',-1,'\$1')'
    #Remove copied file since temp disks vary from node to node
    rm \$name
    rm \$tempdir/\$name
    ls -a
else
    echo "ERROR:copy failed or file '\$tempdir/\$name' does not exist!"
endif
EOF

    print $fh $macro_text;
    close $fh;
    #Need to give execute permissions otherwise condor won't be able to run it
    system( "/usr/bin/chmod 755 $FullFileName" )==0 or die "Unable to give execute permisions to CshellMacro: $!\n";
}

sub CompareOutput
{
    my $DirHash = shift;
    my $print = shift;
    #Remove trailing '/'
    my $char = chop $DirHash;
    while( $char eq "/" ){$char = chop $DirHash;}
    $DirHash = $DirHash.$char;

    opendir my $dh, $DirHash or die "Could not open '$DirHash' for reading '$!'";

    my %AllIters;
    my %OutIters;
    while( my $item = readdir $dh ){
	#print "$item\n";
	if( $item =~ m/Summary_\w*\.list/ ){
	    open( my $summary_fh, '<', "$DirHash/$item" ) or die "Could not open file '$item' for reading: $!";
	    my $joblevel = 0;
	    while( my $line = <$summary_fh> ){
		chomp $line;
		if( $line =~ m/\/[\0-9a-zA-Z_]*\.MuDst\.root/ ){
		    my $iter = substr($line,-18);
		    $iter =~ s/.MuDst.root//;
		    $AllIters{$iter} = $joblevel;
		    print "$line | $iter | $joblevel\n";
		    $joblevel++;
		}
	    }
	}
	if( $item eq "Output" && -d "$DirHash/Output" ){  #Output directory for job
	    opendir my $output_dh, "$DirHash/Output" or die "Could not open '$DirHash' for reading '$!'";
	    while( my $outfile = readdir $output_dh ){
		if( $outfile =~ m/SimpleTree\w*.root/ ){
		    my $outiter = $outfile;
		    $outiter =~ s/SimpleTree_//;
		    $outiter =~ s/.root//;
		    print "$outfile | $outiter\n";
		    $OutIters{$outiter} = 1;
		}
	    }
	    closedir $output_dh;
	}
    }
    
    closedir $dh;

    if( $print>=2 ){ print "Missing iterations\n #. iteration number | job number\n"; }
    my %MissingJobs;
    foreach my $iter (keys %AllIters){
	if( ! $OutIters{$iter} ){
	    $MissingJobs{ $AllIters{$iter} } = 1;
	    if( $print>=2 ){ print " ".scalar(keys %MissingJobs).". $iter | $AllIters{$iter}\n"; }
	}
    }
    return %MissingJobs;
}

