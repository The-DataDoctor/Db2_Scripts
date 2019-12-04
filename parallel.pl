#!/usr/bin/perl

$|++; # flush output, no caching of STDOUT or STDERR

########################################################################################
# IMPORT AREA                                                                          #
########################################################################################
use strict;
use threads;
use Thread::Semaphore;
use threads::shared;
########################################################################################

########################################################################################
# VARIABLE DECLARATION                                                                 #
########################################################################################
my $max_threads = 3; # default amount of threads.
my @threads;
my $curr_tier : shared = 0;
my @thread_tiers : shared;
my @threads_running : shared;

my @commands : shared;
my $buffed_command : shared;
my $curr_command : shared = 0;
my $error_end : shared = 0;

my $filename;

my $semaphore = new Thread::Semaphore; # standard semaphore
my $log_semaphore = new Thread::Semaphore; # Semaphore for log printing

my $switch_tier : shared = 1;
########################################################################################

########################################################################################
# PARSE_ARGUMENTS()                                                                    #
# This sub parses the arguments given to the script and stores them in the appropriate #
# variables. If an unknown argument arrives this sub will end the whole script!        #
########################################################################################
sub parse_arguments
{
	if ( scalar(@ARGV) == 0 )
	{
		print "No arguments given\n";
		print_usage();
		exit(0);
	}
	for ( my $i = 0; $i<scalar(@ARGV); $i++) {
    if ( $ARGV[$i] eq "-h" or $ARGV[$i] eq "-?" or $ARGV[$i] eq "-help")
    {
    	print_usage();
    	exit(0);
    }
    if ( $ARGV[$i] eq "-p" )
    {
    	$i = $i + 1;
    	$max_threads = $ARGV[$i];
    	next;
    }
    if ( $ARGV[$i] eq "-i" )
    {
    	$i = $i + 1;
    	$filename = $ARGV[$i];
    	next;
    }
    print STDERR "Unknown Argument $ARGV[$i]\n";
    print_usage();
    exit(-1);
  }
}
########################################################################################

########################################################################################
# PREREQUISITES()                                                                      #
# All needed prerequisites before executing are tested here                            #
########################################################################################
sub  prerequisites
{
	if ( !defined $filename )
	{
		print "You have to give an input filename with '-i'!";
		print_usage();
		exit(-1);
	}
}
########################################################################################

########################################################################################
# PRINT_USAGE()                                                                        #
# This sub prints out the usage notes for this script.                                 #
########################################################################################
sub print_usage
{
  print "USAGE parallel.pl\n";
  print "\t-h/-?\tShows this help\n";
  print "\t-i\tinput file with one command per line and tier preceeded with '\@\@' at the end\n";
  print "\t\te.g. date\@\@0\n";
	print "\t-p\tParallelism. Max amount of commands executed in parallel (default 3)\n\n";
  print "\t\t There could be additional parameters for a LINE after the tier information\n";
  print "\t\t Additional parameters have to be seperated by ;\n";
  print "\t\t Possible parameters:\n";
  print "\t\t !\t Ends complete script on error of this line (rc !=0)\n";
  print "\t\t !<NUMBER>\t substitute <NUMBER> by an allowed return code for the line. You could put this parameter multiple times behind a line\n";
  print "\t\t #\thide this line in log output\n\n";
}
########################################################################################

########################################################################################
# DOING(THREAD_ID)                                                                     #
# THREAD_ID is the ID for the thread                                                   #
# This is the sub executed by every Thread!                                            #
# It is getting a command from the get_line() sub and executes it.                     #
# If an empty string is given back the Thread will sleep.                              #
# If the command string is only "END" the Thread will end itself.                      #
########################################################################################
sub doing
{
  my $thread_id = $_[0];
  while (1==1)
  {
    $semaphore->down;
    my $exec_line = get_line($thread_id);
    $semaphore->up;
    while ( $exec_line eq "" )
    {
      $log_semaphore->down;
      print_log($thread_id, "Going to sleep");
      $log_semaphore->up;

      lock($switch_tier);
      cond_wait($switch_tier);

      $log_semaphore->down;
      print_log($thread_id, "Awake");
      $log_semaphore->up;

      $semaphore->down;
      $exec_line = get_line($thread_id);
      $semaphore->up;
    }
    if ( $exec_line eq "END" )
    {
      $log_semaphore->down;
      print_log($thread_id, "Ending ...");
      $log_semaphore->up;
      cond_broadcast($switch_tier);
      return;
    }
    $exec_line =~ /^(.+)\@\@(\d+)(.*)/;
    my $command = ();

    $command->{cmd} = $1;
    $command->{tier} = $2;
    foreach my $prm (split(/;/,$3))
    {
      if ($prm eq "!")
      {
        $command->{end_on_error} = 1;
        push(@{$command->{allowed_rc}}, 0);
        next;
      }
      elsif ($prm eq "#")
      {
        $command->{hide} = 1;
        next;
      }
      elsif ( $prm =~ m/\!(\d+)/ )
      {
        $command->{end_on_error} = 1;
        push(@{$command->{allowed_rc}}, $1);
        next;
      }
    }
    $log_semaphore->down;
    if ( defined $command->{hide} && $command->{hide} == 1 )
    {
      print_log($thread_id, "Executing: <hidden command>; command was marked to be hidden in output");
    }
    else
    {
      print_log($thread_id, "Executing: $command->{cmd}");
    }
    $log_semaphore->up;
    my $out = `$command->{cmd}`;
    my $rc = $?;
    if ( defined $command->{end_on_error} && $command->{end_on_error} == 1 )
    {
      my $allowed = 0;
      foreach my $my_rc (@{$command->{allowed_rc}})
      {
        if ( $rc eq $my_rc )
        {
          $allowed = 1;
        }
      }
      if ( $allowed == 0 )
      {
        $log_semaphore->down;
        print_log($thread_id, "Unallowed RC $rc");
        $log_semaphore->up;
        $error_end = 1;
      }
    }
    $log_semaphore->down;
    if ( defined $command->{hide} && $command->{hide} == 1 )
    {
      print_log($thread_id, "<hidden command>; command was marked to be hidden in output");
    }
    else
    {
      print_log($thread_id, "$command->{cmd}");
    }
    print_log($thread_id, "$out");
    print_log($thread_id, "RC: $rc");
    if ( defined $command->{end_on_error} && $command->{end_on_error} == 1 )
    {
      print_log($thread_id, "Allowed RCs are: @{$command->{allowed_rc}}");
    }
    $log_semaphore->up;
  }
}
########################################################################################

########################################################################################
# GET_LINE()                                                                           #
# This sub returns a command to be executed to the invoking thread.                    #
# If the new command is on another tier than those currently running it returns an     #
# empty command which lets the thread sleep.                                           #
# If there are no more commands to be executed it return "END" to the thread.          #
########################################################################################
sub get_line
{
  my $curr_thread=$_[0];
  $thread_tiers[$curr_thread] = "";
  if ( $error_end == 1 )
  {
    $log_semaphore->down;
    print_log("MAIN", "Process End On Error was chased!!!");
    $log_semaphore->up;
    return "END";
  }
  if ( $buffed_command eq "") # No command saved, take a new one out of the array
  {
    if ( scalar(@commands)<=$curr_command ) # no more commands to be executed
    {
      return "END";
    }
    my $line = $commands[$curr_command];
    $curr_command = $curr_command+1;
    my $tot_cmds = scalar(@commands);
    $line =~ /^(.+)\@\@(\d+)(.*)/;
    #my $command = $1;
    my $line_tier = $2;
    $log_semaphore->down;
    print_log("MAIN", "Getting command id $curr_command being in tier $line_tier. Max ID is $tot_cmds");
    $log_semaphore->up;
    if ( $line_tier eq $curr_tier ) # new command is in current tier --> execute it
    {
      $thread_tiers[$curr_thread] = $line_tier;
      return $line;
    }
    else # new command is not in current tier
    {
      my $wait = 0;
      for (my $i = 0; $i<scalar(@threads);$i++) # is there a thread still executing current tier?
      {
        if ($thread_tiers[$i] ne "")
        {
          $wait = 1;
        }
      }
      if ( $wait == 1 ) # there is still a thread executing current tier --> buff command and let thread sleep
      {
        $buffed_command = $line;
        $threads_running[$curr_thread] = 0;
        return "";
      }
      else # no thread anymore in current tier --> switch curr_tier to new tier and let thread execute command
      {
        $thread_tiers[$curr_thread] = $line_tier;
        $curr_tier=$line_tier;

        $log_semaphore->down;
        print_log("MAIN", "Switching to tier $curr_tier");
        $log_semaphore->up;
        
        return $line;
      }
    }
  }
  else # there is a saved command in $buffed_command, means a new tier will start
  {
    my $wait = 0;
    for (my $i = 0; $i<scalar(@threads);$i++) # is there a thread still executing current tier?
    {
      if ($thread_tiers[$i] ne "")
      {
        $wait = 1;
      }
    }
    if ( $wait == 1 ) # there is still a thread executing current tier --> let requesting thread sleep
    {
      $threads_running[$curr_thread] = 0;
      return "";
    }
    else # no thread anymore in current tier --> switch curr_tier to new tier and let requesting thread execute buffed command
    {
      $buffed_command =~ /^(.+)\@\@(\d+)(.*)/;
      my $line_tier = $2;
      $thread_tiers[$curr_thread] = $line_tier;
      $curr_tier = $line_tier;
      my $command = $buffed_command;
      $buffed_command = "";
      $log_semaphore->down;
      print_log("MAIN", "Switching to tier $curr_tier");
      $log_semaphore->up;
      cond_broadcast($switch_tier);
      return $command;
    }
  }
}
########################################################################################

########################################################################################
# INIT()                                                                               #
# This sub initiates the script.                                                       #
# It reads all lines from the input line and stores it in the command array            #
########################################################################################
sub init
{
  open(IN,'<'.$filename) || die "Can not open file $filename: $!";
  while ( my $line = <IN> )
  {
    chomp($line);
    if ( ($line ne "" ) && ( $line =~ m/^(.+)\@\@(\d+).*/) )
    {
      $commands[scalar(@commands)] = $line;
    }
  }
  close(IN);
}
########################################################################################

########################################################################################
# PRINT_LOG(THREAD_ID, OUTPUT)                                                         #
# THREAD_ID is the ID of the thread invoking the sub.                                  #
# OUTPUT are the lines to be printed                                                   #
# Prints out a log entry                                                               #
########################################################################################
sub print_log
{
  my $thread_out = $_[0];
  my $output = $_[1];
  my $now_string = localtime;
  my @out_arr = split(/\n/, $output);
  for (my $o = 0; $o<scalar(@out_arr); $o++)
  {
    chomp($out_arr[$o]);
    print "$now_string\t$thread_out\t$out_arr[$o]\n";
  }
}
########################################################################################

########################################################################################
# MAIN()                                                                               #
# This is just the entry point for the script                                          #
# The given amount of threads is started here and waited that they end                 #
########################################################################################
sub main
{
  init();
  for ( my $j = 0; $j < $max_threads; $j++ )
  {
    $log_semaphore->down;
    print_log("MAIN", "Starting Thread $j.");
    $log_semaphore->up;
    $threads[$j] = threads->new(\&doing, $j);
    $threads_running[$j] = 1;
  }
  
  for ( my $j = 0; $j < $max_threads; $j++ )
  {
    $threads[$j]->join;
    $log_semaphore->down;
    print_log("MAIN", "Thread $j ended.");
    $log_semaphore->up;
  }
}
########################################################################################

parse_arguments();
prerequisites();
main();
