#!/usr/bin/perl

use strict;
use warnings;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Cwd qw(abs_path);
use File::Spec;
use Getopt::Long qw(GetOptions);
use Getopt::Long qw(HelpMessage);
use Pod::Usage;

=pod

=head1 NAME

submit_event_scheduler_fcsgain_new - Handy script for submitting FCS Pi0 analysis jobs using STAR scheduler

=head1 SYNOPSIS

submit_event_scheduler_fcsgain_new.pl [option] [value] ...

  --data, -d       Directory where output data will be stored (default is directory of macro)
  --runnumber, -r  (required) Runnumber for analysis
  --list, -l       only generate a file list for the given run number in the directory given by '-d'
  --test, -t       Test only
  --verbose, -v    level set the printout level (default is 1)
  --help, -h       print this help

=head1 VERSION

0.1

=head1 DESCRIPTION

This program will create the neccessary directories and files for where the output of the Pi0 analysis will go and submit the job to the STAR scheduler. It will also remove any files from the old analysis if they exist in that directory

=cut

=begin comment
Author: David Kapukchyan

This is a modification of submt_event_scheduler_fcsgain_.pl originally written by Xilin Liang. It's purpose is to submit jobs to the STAR scheduler for FCS Pi0 calibration. It is modified so that scheduler jobs can be submitted on a seperate directory other than run22root

@[December 16, 2022]
> First instance
=cut

#Here is the setup to check if the submit option was given

my $LOC = $ENV{'PWD'};
#my $OUTDIR = $LOC;
my $VERBOSE = 1;
my $LIST;
my $TEST;
my $folder="$Bin";
while( chop($folder) ne "/" ){ }
my $DATADIR = $folder;
my $RUN;

GetOptions(
    'datadir|d=s'    => \$DATADIR,
    'runnumber|r=s'  => \$RUN,
    'list|l'         => \$LIST,
    'test|t'         => \$TEST,
    'verbose|v=i'    => \$VERBOSE,
    'help|h'         => sub { HelpMessage(0) }
    ) or HelpMessage(1);

if( !$RUN || $RUN !~ m/\d{8}/ ){ print "ERROR:Invalid run number:$RUN\n"; HelpMessage(0); }

if( $VERBOSE >=1 ){ print "Working folder = ${folder}\n"; }

my $char = chop $DATADIR; #Get last character of DATADIR
while( $char eq "/" ){$char = chop $DATADIR;} #Remove all '/'
$DATADIR = $DATADIR.$char;     #Append removed character which was not a '/'
$DATADIR = abs_path($DATADIR); #Get absolute path

if( $TEST ){exit(0);}

if( $VERBOSE>=1 ){ print "Data folder = $DATADIR\n"; }

my $day = substr($RUN,2,3);
my $yy = substr($RUN,0,2);
my $year = $yy+1999;   #quick way to get year from run number
if( $VERBOSE>=1 ){
    print "run number = $RUN\n";
    print "year = $year\n";
}

my $mudstroot="/star/data1*/reco/production_pp500_2022/ReversedFullField/pp500_22_DEV_fcs/$year/$day/$RUN/st_fwd*.MuDst.root";
if( $VERBOSE>=1 ){
    print "MuDst files location =  $mudstroot\n";
}
if( $LIST ){
    if( $VERBOSE>=1 ){ print "Creating filelist\n"; }
    my $filelist="$DATADIR/$RUN"."eventroot.list";
    if (-f $filelist) { system("/bin/rm $filelist") == 0 or die "Unable to remove file in '$filelist': $!"; }
    system("ls $mudstroot > $filelist") == 0 or die "Unable to create file list '$filelist': $!";
    exit(0);
}

my $pi0root="$DATADIR/$RUN";
if( $VERBOSE>=1 ){ print "Checking/Creating $pi0root\n"; }
if (not -d "$pi0root") {system("/bin/mkdir -p $pi0root") == 0 or die "Unable to make folder '$pi0root': $!";}

my $log="$DATADIR/log";
if( $VERBOSE>=1 ){ print "Checking/Creating $log\n"; }
if (not -d "$log") {system("/bin/mkdir -p $log") == 0 or die "Unable to make folder '$log': $!"; }

opendir my $dh, $pi0root or die "Unable to open directory '$pi0root': $!";
my @files = readdir $dh;
if( scalar(@files)>2 ){
    print "Remove files in $pi0root (Y/n):";
    my $input = <STDIN>; chomp $input;
    if( $input eq "Y" ){
	foreach my $file (@files){
	    if( $file =~ m/StFcsPi0invariantmass${RUN}_\w*.root/ ){
		if( $VERBOSE>=2 ){print "removing $pi0root/$file\n";}
		system("/bin/rm $pi0root/$file") == 0 or die "Unable to remove file in '$file': $!";
	    }
	}
    }
    else{ print "Keeping files in $pi0root\n"; }
}
closedir $dh;

if( $VERBOSE>=1 ){ print "Creating filelist\n"; }
my $filelist="$DATADIR/$RUN"."eventroot.list";
if (-f $filelist) { system("/bin/rm $filelist") == 0 or die "Unable to remove file in '$filelist': $!"; }
system("ls $mudstroot > $filelist") == 0 or die "Unable to create file list '$filelist': $!";

my $submit = "Y";
if( $VERBOSE>=1 ){
    $submit = "n";
    print "Submit job (Y/n):";
    $submit = <STDIN>; chomp($submit);
}
if( $submit eq "Y" ){
    system("star-submit-template -template $folder/submitScheduler/submitScheduler_dataevent_run22_new.xml -entities folder=$folder,datafolder=$DATADIR,runnumber=$RUN")==0 or die "Unable to submit job to STAR scheduler: $!";
}

