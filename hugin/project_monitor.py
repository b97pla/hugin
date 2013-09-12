
import os
import glob
import datetime
from hugin.monitor import Monitor
    
RUN_PROCESS_STEPS = ["bcbb analysis started",
                     "bcbb analysis completed",
                     "sample qualities evaluated",
                     "sample species confirmed by blast",
                     "customer uppnex id verified",
                     "raw data delivered to customer inbox",
                     "sample status note generated",
                     "project status note generated",
                     "delivery notes copied to customer inbox",
                     "delivery email sent to customer and application specialists",
                     "delivery marked in Genomics Project List"
                     ]

BEST_PRACTICE_ANALYSIS_STEPS = ["data from sample runs merged",
                                 "best practice analysis started",
                                 "best practice analysis completed",
                                 "best practice analysis results delivered to customer inbox",
                                 "best practice analysis report generated",
                                 "best practice analysis report copied to customer inbox",
                                 "best practice analysis delivery marked in Genomics Project List"
                                 ]

PROJECT_COMPLETION_STEPS = ["all raw data delivered",
                            "all raw data delivered marked in Genomics Project List",
                            "best practice analysis delivered",
                            "project finished"]

SEQUENCING_IN_PROGRESS = "Sequencing"
BCBB_ANALYSIS_IN_PROGRESS = "bcbb analysis"
DELIVERY_IN_PROGRESS = "Delivery"
BP_IN_PROGRESS = "Best practice"
PROJECT_FINISHED = "Finished"
STALLED = "Check status"
ABORTED = "Aborted"

# The number of seconds we allow the bcbb logfile to be inactive before we flag the project as stalled
BCBB_LOGFILE_INACTIVE = datetime.timedelta(seconds=60*60*3)
BCBB_ANALYSIS_DURATION = datetime.timedelta(seconds=60*60*24*2)
DELIVERY_DURATION = datetime.timedelta(seconds=60*60*24*5)

class ProjectMonitor(Monitor):
    
    def __init__(self, config):
        super(ProjectMonitor, self).__init__(config)
        self.trello_board = self.trello.get_board(config.get("trello",{}).get("project_tracking_board",None),True)
        assert self.trello_board is not None, "Could not locate project tracking board in Trello"
        self.archive_folders = [d.strip() for d in config.get("archive_folders","").split(",")]
        self.analysis_folders = [d.strip() for d in config.get("analysis_folders","").split(",")]
        self.samplesheet_folders = [d.strip() for d in config.get("samplesheet_folders","").split(",")]
        
    def add_project_card(self, project, status=SEQUENCING_IN_PROGRESS):
        """Add a project card"""
        
        card = self.trello.get_card_on_board(self.trello_board, project)
        lst = self.trello.add_list(self.trello_board, status)
        if card is None:
            desc = self.get_project_metadata(project)
            card = self.trello.add_card(lst,project,desc)
            card.add_checklist("Project", PROJECT_COMPLETION_STEPS)
            card.add_checklist("Best practice analysis", BEST_PRACTICE_ANALYSIS_STEPS)
        
        card.set_closed(False)
        card.change_list(lst.id)
        return card
    
    def add_run_to_project(self, project, run):
        """Add a run to a project card
        """
        
        card = self.add_project_card(project)
        # Fetch the checklists on this card
        card.fetch() 
        if run['short_name'] not in [chklst.name for chklst in card.checklists]:
            card.add_checklist(run['short_name'], RUN_PROCESS_STEPS)
        
        # Make sure to uncheck any incompatible completed events
        for chklst in card.checklists:
            if chklst.name in ["Project","Best practice analysis"]:
                for item in chklst.items:
                    chklst.set_checklist_item(item.get('name',''),False)

    def get_run_status(self, run):
        """Check if all projects and samples in a run has been transferred to the analysis folder
        """
        ssheet = run['samplesheet']
        if ssheet is None:
            print("Could not locate samplesheet for run {}".format(run['name']))
            return False
        
        for sample_data in ssheet:
            if not self.get_sample_analysis_folder(sample_data['SampleProject'].replace("__","."),
                                                   sample_data['SampleID'],
                                                   "_".join([run['date'],"{}{}".format(run['position'],run['flowcell_id'])])):
                return False
        
        return True

    def update_run_status(self):
        """Update the status of runs on the run tracking board"""
        
        # Create a RunMonitor object to update the run tracking board
        from hugin.run_monitor import RunMonitor
        rm = RunMonitor(self.config)
        
        rm.run_folders = self.archive_folders
        rm.samplesheet_folders = []
        
        # Loop over the runs and check whether all samples and projects have been transferred to the 
        # analysis folder
        for run in rm.list_runs():
            print("Checking run {}".format(run['name']))
            if self.get_run_status(run):
                rm.set_run_completed(run)
        
    def set_card_checklist_item(self, card, chklist_name, item_name, state):
        """Mark the bcbb analysis as started for a project and run"""
        try:
            [chklist] = [c for c in card.checklists if c.name == chklist_name]
            chklist.set_checklist_item(item_name,state)
        except ValueError:
            return None
        
    def get_sample_analysis_folder(self, project, sample, run_id):
        sample_dir = os.path.join(project,sample,run_id)
        for analysis_folder in self.analysis_folders:
            path = os.path.join(analysis_folder,sample_dir)
            if os.path.exists(path):
                return path
        return None
          
    def get_project_metadata(self, project):
        return ""

    def _list_folders(self, pattern, path):
        folders = []
        for fname in os.listdir(path):
            m = re.match(pattern,fname)
            fpath = os.path.join(path,fname)
            if not (m and os.path.exists(fpath) and os.path.isdir(fpath)):
                continue
            folders.append(fpath)
        return folders
        
    def list_flowcells(self, path):
        """Get a list of the flowcells in a sample folder"""
        pattern = r'(\d{6})_([AB]?)([A-Z0-9\-]+)'
        fcs = []
        for path in self._list_folders(pattern,path):
            m = re.match(pattern,os.path.basename(path))
            if not m or len(m.groups()) != 3:
                continue
            try:
                datetime.datetime.strptime(m.group(1),"%y%m%d")
            except ValueError:
                continue
            
            fc = {'path': path,
                  'name': os.path.basename(path),
                  'date': m.group(1),
                  'position': m.group(2),
                  'flowcell_id': m.group(3)}
            fcs.append(fc)
        return fcs

    def list_samples(self, path):
        """Get a list of the samples in a project folder"""
        pattern = r'.*'
        samples = []
        for path in self._list_folders(pattern,path):
            sample = {'path': path,
                      'name': os.path.basename(path),
                      'flowcells': self.list_flowcells(path)}
            if len(sample['flowcells']) == 0:
                continue
            samples.append(sample)
        return samples
            
    def list_projects(self):
        """Get a list of the projects in the analysis folder"""
        
        pattern = r'[A-Za-z\._]+\d{2}_\d{2,}'
        projects = []
        for analysis_folder in self.analysis_folders:
            for path in self._list_folders(pattern,analysis_folder):
                project = {'path': path,
                           'name': os.path.basename(path),
                           'samples': self.list_samples(path)
                           }
                projects.append(project)
        return projects
    
    def update_trello_board(self):
        """Update the Trello board based on the contents of the analysis folder"""
         # Don't update the card if it is in any of these lists
        skip_list_ids = [self.trello.get_list_id(self.trello_board,PROJECT_FINISHED),
                         self.trello.get_list_id(self.trello_board,ABORTED)]
        projects = self.list_projects()
        for project in projects:
            print("Adding project {}".format(project['name']))
            status, due = self.get_status_due(project)
      
            # If due time has passed, set status to stalled
            if due < datetime.datetime.utcnow():
                status = STALLED
                
            card = self.trello.get_card_on_board(self.trello_board,project['name'])
            # Don't create any new cards, rely on these being created by the run_monitor process
            if card is None:
                continue
            
            # Gather the information on the project and update the description on the card as necessary
            metadata = self.get_project_metadata(project)
            self.set_description(card,metadata,True)
            self.set_due(card,due)
            
            # If the card was moved to the STALLED list, send a notification                
            if self.trello.change_list(card, status, skip_list_ids) and status == STALLED:
                users = [self.trello.client.get_member(mid) for mid in card.member_ids]
                self.send_status_notification(project,status,users)
            
    def get_status_due(self, project):
        """Determine the processing status of a project
        """
        status_flags = ["01_analysis_start.txt",
                        "project-summary.csv"]
        statuses = {}
        for sample in project["samples"]:
            for flowcell in sample["flowcells"]:
                status = STALLED
                if flowcell["name"] not in statuses:
                    statuses[flowcell["name"]] = []
                if os.path.exists(os.path.join(flowcell["path"],status_flags[1])):
                    status = DELIVERY_IN_PROGRESS
                    due = datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(flowcell["path"],
                                                                                        status_flags[1]))) + DELIVERY_DURATION
                elif os.path.exists(os.path.join(flowcell["path"],status_flags[0])):
                    logfile = os.path.join(flowcell["path"],"{}-bcbb.log".format(sample["name"]))
                    due = datetime.datetime.utcnow()
                    if os.path.exists(logfile) and due - datetime.datetime.fromtimestamp(os.path.getmtime(logfile)) < BCBB_LOGFILE_INACTIVE:
                         status = BCBB_ANALYSIS_IN_PROGRESS
                         due = datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(flowcell["path"],
                                                                                             status_flags[0]))) + BCBB_ANALYSIS_DURATION
                statuses[flowcell["name"]].append([status,due])
        
        return None, None
            
