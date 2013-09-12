
import os
import re
import csv
import glob
import datetime
from hugin.monitor import Monitor

FIRSTREAD = "First read"
INDEXREAD = "Index read"
SECONDREAD = "Second read"
PROCESSING = "Processing"
UPPMAX = "Sent to Uppmax"
COMPLETED = "Handed over"
STALLED = "Check status"
ABORTED = "Aborted"

PER_CYCLE_MINUTES = {'RapidRun': 12,
                     'HighOutput': 100,
                     'MiSeq': 6}
  
class RunMonitor(Monitor):
    
    def __init__(self, config):
        super(RunMonitor, self).__init__(config)
        self.trello_board = self.trello.get_board(config.get("trello",{}).get("run_tracking_board",None),True)
        assert self.trello_board is not None, "Could not locate run tracking board in Trello"
        self.run_folders = [d.strip() for d in config.get("run_folders","").split(",")]
        self.samplesheet_folders = [d.strip() for d in config.get("samplesheet_folders","").split(",")]
        self.instruments = config.get("instruments", {})
        
    def set_run_completed(self, run):
        """Set the status of the run to completed"""
        card = self.trello.get_card_on_board(self.trello_board,run['name'])
        
        # Skip if the card is not on the board or if it has been closed
        if card is None or card.closed:
            return

        lst = self.trello.add_list(self.trello_board,COMPLETED)
        if card.list_id != lst.id:
            card.change_list(lst.id)

    def get_due_datetime(self, run, step, started=datetime.datetime.utcnow()):
        """Get the expected due date for a particular sequencing/processing step"""
        
        # Return None if we are not familiar with the step
        if step not in ["Pre-seq",FIRSTREAD,INDEXREAD,SECONDREAD,PROCESSING,UPPMAX]:
            return None
        
        # processing step
        if step == PROCESSING:
            max_duration = 8*60*60
        # uploaded to uppmax but not handed over
        elif step == UPPMAX:
            max_duration = 3*60*60
        # sequencing steps
        else:
            # Get the expected length of a cycle in seconds
            cycle_duration = 60 * PER_CYCLE_MINUTES[run['run_parameters'].get('RunMode','HighOutput' if not self.is_miseq_run(run) else 'MiSeq')]
            reads = run['run_info'].get('Reads',[])
            index_cycles = [int(r.get('NumCycles','0')) for r in reads if r.get('IsIndexedRead','N') == 'Y']
            read_cycles = [int(r.get('NumCycles','0')) for r in reads if r.get('IsIndexedRead','N') == 'N']
            if step == "Pre-seq":
                max_duration = 2 * cycle_duration
            elif step == INDEXREAD:
                max_duration = max(index_cycles) * cycle_duration
            elif step == FIRSTREAD:
                max_duration = read_cycles[0] * cycle_duration
            else:
                max_duration = read_cycles[1] * cycle_duration
                
        return started + datetime.timedelta(seconds=max_duration)

    def get_status_due(self, run):
        """Get the current status and the maximum expected due time for a run"""
        
        reads = run['run_info'].get('Reads',[])
        status_flags = ["First_Base_Report.htm"] + ["Basecalling_Netcopy_complete_Read{}.txt".format(str(i+1)) for i in range(len(reads))]
        started_processing_flags = ["initial_processing_started.txt", "first_read_processing_started.txt", "second_read_processing_started.txt"] 
        completed_processing_flags = [f.replace("started","completed") for f in started_processing_flags]
        
        # Get number of non-index reads
        treads = len([r for r in reads if r.get("IsIndexedRead","N") == "N"])
        
        # Get the highest file flag
        last_event_flag = run['path']
        for f in status_flags:
            f = os.path.join(run['path'],f)
            if os.path.exists(f):
                last_event_flag = f
            else:
                break
         
        try: 
            flag_index = status_flags.index(os.path.basename(last_event_flag))
        except ValueError:
            flag_index = -1
        
        # flag_index will be equal to the read number that has been finished
        # thus we are currently sequencing read flag_index+1
        
        # first base report has not yet appeared
        if flag_index < 0:
            status = FIRSTREAD
            due = self.get_due_datetime(run, "Pre-seq" if not self.is_miseq_run(run) else status, started=datetime.datetime.fromtimestamp(os.path.getmtime(last_event_flag)))
        else:
            # sequencing has finished
            if flag_index == len(reads):
                complete_flag = os.path.join(run['path'],completed_processing_flags[treads])
                start_flag = os.path.join(run['path'],started_processing_flags[treads])
                if os.path.exists(complete_flag):
                    status = UPPMAX
                    last_event_flag = complete_flag
                elif os.path.exists(start_flag):
                    status = PROCESSING
                    last_event_flag = start_flag
                else:
                    status = PROCESSING
            # in the middle of sequencing
            else:
                # sequencing an index read
                if [r for r in reads if int(r["Number"]) == (flag_index + 1)][0].get("IsIndexedRead","N") == "Y":
                    status = INDEXREAD
                # sequencing a non-index read
                else:
                    # Count the number of non-index reads that have already been sequenced
                    if len([r for r in reads if int(r["Number"]) <= flag_index and r.get("IsIndexedRead","N") == "N"]) == 0:
                        status = FIRSTREAD
                    else:
                        status = SECONDREAD

            # get the expected due date
            due = self.get_due_datetime(run, status, started=datetime.datetime.fromtimestamp(os.path.getmtime(last_event_flag)))
        
        return status, due

    def update_trello_board(self):
        """Update the Trello board based on the contents of the run folder
        """
        # Don't update the card if it is in any of these lists
        skip_list_ids = [self.trello.get_list_id(self.trello_board,COMPLETED),
                         self.trello.get_list_id(self.trello_board,ABORTED)]
        runs = self.list_runs()
        for run in runs:
            print("Adding run {}".format(run['name']))
            status, due = self.get_status_due(run)
            
            # If due time has passed and didn't find all status files, set status to stalled.
            # if status == UPPMAX means that the processing has finished, and should have more
            # priority than due date
            if due < datetime.datetime.utcnow() and status != UPPMAX:
                status = STALLED
                
            card = self.trello.get_card_on_board(self.trello_board,run['name'])
            if card is None:
                card = self.trello.add_card(self.trello.add_list(self.trello_board,status), run['name'])
                was_moved= True
            else:
                was_moved = self.trello.change_list(card, status, skip_list_ids)
                card.set_closed(False)
            
            # Gather the information on the run and update the description on the card as necessary
            metadata = self.get_run_metadata(run)
            self.set_description(card,metadata,True)
            self.set_due(card,due)
            
            # If the card was moved to the STALLED list, send a notification                
            if status == STALLED and was_moved:
                users = [self.trello.client.get_member(mid) for mid in card.member_ids]
                self.send_status_notification(run,status,users)
                
    def update_trello_project_board(self):
        """Update the project cards for projects in ongoing runs
        """
        from hugin.project_monitor import ProjectMonitor
        pm = ProjectMonitor(self.config)
        runs = self.list_runs()
        for run in runs:
            projects = self.get_run_projects(run)
            for project in projects:
                print("Adding run {} to project {}".format(run['name'],project))
                pm.add_run_to_project(project,run)
    
    def send_status_notification(self, run, status, users=[]):
        """Send an email notification that a run has been moved to a list
        """
        subject = "[hugin]: The run {} needs attention".format(run['name'])
        msg = "The run {} has been moved to the '{}' list on the Trello board "\
              "'{}' and may need your attention.".format(run['name'],
                                                         status,
                                                         self.trello_board.name)
        self.send_notification(subject,msg,users)
      
    def get_run_metadata(self, run):
        metadata = {}
        metadata['Projects'] = run['projects']
        metadata['Setup'] = self.get_run_setup(run) 
        metadata['Flowcell'] = run['run_info'].get('Flowcell','NA')
        instrument = run['run_info'].get('Instrument','NA')
        if self.instruments:
            instrument_name = self.instruments.get(instrument, '')
            if instrument_name:
                instrument += " ({name})".format(name=instrument_name)
        metadata['Instrument'] = instrument
        metadata['Date'] = run['run_info'].get('Date','NA')      
        metadata['Run mode'] = run['run_parameters'].get('RunMode','HighOutput' if not self.is_miseq_run(run) else 'MiSeq')  
        return metadata
    
    def get_run_setup(self, run):
        reads = run['run_info'].get('Reads',[])
        read_cycles = [r.get('NumCycles','?') for r in reads if r.get('IsIndexedRead','N') == 'N']
        index_cycles = [r.get('NumCycles','?') for r in reads if r.get('IsIndexedRead','N') == 'Y']
        nreads = len(read_cycles)
        nindex = len(index_cycles)
        
        c = list(set(read_cycles))
        if len(c) == 1:
            setup = "{}x{}".format(str(nreads),c[0])
        else:
            setup = ",".join(c)
        
        return setup
