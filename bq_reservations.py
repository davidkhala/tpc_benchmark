"""BigQuery Reservations API Client

MIT License, see LICENSE file for complete text.
Copyright (c) 2020 SADA Systems, Inc.
"""

import google.cloud.bigquery.reservation_v1 as bqr

import config


class BQR:
    def __init__(self, fp_service_account_key, project, location):
        """Create a slot reservation on BigQuery
        
        Usage
        -----
        Creating an assignment requires the following sequence of steps and 
        class methods:
        1. buy commitment : self.commitment
        2. create reservation : self.reservation
        3. assign reservation to project : self.assignment
        
        Deleting a slot purchase commit requires the opposite sequence:
        1. delete all assignments for a reservation : self.delete_commitment 
        2. delete all reservations for a commit : self.delete_reservation
        3. delete commit : self.delete_commitment
        
        Parameters
        ----------
        fp_service_account_key : str, absolute path to JSON service account key
        project : str, the parent resource name of the assignment e.g.
            ``projects/myproject/locations/US/reservations/team1-prod``
            This does not need to be the project that will consume the slot
            reservation.
        location : str, the location of the project, i.e. `US` or `EU` etc. This does
            need to be the same as the consuming project specified in an assignment.
        """
        self.fp_service_account_key = fp_service_account_key
        self._res_api = bqr.ReservationServiceClient.from_service_account_json(fp_service_account_key)
        self.project = project.lower()  # always lower!
        self.location = location
        self.parent_arg = f"projects/{self.project}/locations/{self.location}"
        
        self.commit_config = None
        self.reservation_config = None
        
        self.last_commitment = None
        self.last_reservation = None
        self.last_assignment = None
        
        self.list_commitments = None
        self.list_reservations = None
        self.list_assignments = None
        
        self.verbose = False
        
    def commitment(self, slots, plan='FLEX'):
        """Purchase a slot commitment
        
        Parameters
        ----------
        slots : int, number of slots to purchase, in multiples of 100
        plan : str, one of: 'FLEX', 'ANNUAL', 'MONTHLY', TRIAL', defaults to 'FLEX'
        """
        commit_config = bqr.CapacityCommitment(plan=plan, slot_count=slots)
        
        commit = self._res_api.create_capacity_commitment(parent=self.parent_arg,
                                                          capacity_commitment=commit_config)
        
        if self.verbose:
            print(commit)
            
        commitment_id = commit.name
        
        self.last_commitment = commitment_id
        
        return commitment_id
    
    def reservation(self, slots, name, ignore_idle_slots=False):
        """Create a reservation of slots
        
        TODO: decide if ignore_idle_slots should be a class attribute
        
        Parameters
        ----------
        slots : int, number of slots to reserve
        name : str, name of reservation (will label queries).  A valid name can have Lowercase, 
            alphanumerical characters and dashes only (abc123). Name must start with a letter.
            Must be 64 characters or less.
        ignore_idle_slots : bool, TBD
        
        Returns
        -------
        reservation_id : str
        """
        
        res_config = bqr.Reservation(slot_capacity=slots, ignore_idle_slots=ignore_idle_slots)
        
        res = self._res_api.create_reservation(parent=self.parent_arg,
                                               reservation_id=name.lower(),  # always lower!
                                               reservation=res_config)
        
        if self.verbose:
            print(res)
        
        reservation_id = res.name
        
        self.last_reservation = reservation_id
        
        return reservation_id
        
    def assignment(self, reservation_id, project):
        """Assign a reservation to a GCP Project
        
        Parameters
        ----------
        reservation_id : str, reservation_id to assign to a project
        project : str, GCP project to accept the slot reservation
        """
        
        project = project.lower()  # always lower!
        assign_config = bqr.Assignment(job_type='QUERY',
                                       assignee=f"projects/{project}")
        
        assign = self._res_api.create_assignment(parent=reservation_id,
                                                 assignment=assign_config)
        
        if self.verbose:
            print(assign)
            
        assignment_id = assign.name
        
        self.last_assignment = assignment_id
        
        return assignment_id
        
    def assign_slots(self, slots, plan, name, project):
        """Assign slots to a project by buying a commit and applying a reservation
        
        Parameters
        ----------
        slots : int, number of slots to purchase, in multiples of 100
        plan : str, one of: 'FLEX', 'ANNUAL', 'MONTHLY', TRIAL', defaults to 'FLEX'
        str, name of reservation (will label queries).  A valid name can have Lowercase, 
            alphanumerical characters and dashes only (abc123). Name must start with a letter.
            Must be 64 characters or less.
        project : str, GCP project to accept the slot reservation
        """
        self.commitment(slots=slots, plan=plan)
        self.reservation(slots=slots, name=name)
        self.assignment(reservation_id=self.last_reservation, project=project)
        return self.last_assignment
        
    def inventory(self):
        """Inventory the currently purchased slots, currently set reservations and 
        currently assignments of reservations
        
        Returns
        -------
        None: Updates self.list_commitments, self.list_reservations, self.list_assignments
        """
        self.list_commitments = [i.name for i in self._res_api.list_capacity_commitments(parent=self.parent_arg)]
        self.list_reservations = [i.name for i in self._res_api.list_reservations(parent=self.parent_arg)]

        self.list_assignments = []
        for i in list(map(lambda x: x.split("/")[-1], self.list_reservations)):
            _list = [i.name for i in self._res_api.list_assignments(parent=self.parent_arg + "/reservations/" + i)]
            self.list_assignments.extend(_list)
        
    def delete_assignment(self, assignment_id):
        """Delete an assignment of a reservation to a project based on assignment ID
        
        Parameters
        ----------
        assignment_id : str, assignment id applied to a project
        """
        self._res_api.delete_assignment(name=assignment_id)
        
    def delete_reservation(self, reservation_id):
        """Delete a reservation of slots based on reservation ID
        
        Parameters
        ----------
        reservation_id : str, assignment id applied to a project
        """
        self._res_api.delete_reservation(name=reservation_id)
        
    def delete_commitment(self, commitment_id):
        """Delete a slot purchase commitment based on commitment ID
        
        Parameters
        ----------
        commitment_id : str, commitment ID for purchase
        """
        
        self._res_api.delete_capacity_commitment(name=commitment_id)
        
    def delete_last_assignment(self):
        """Delete the last slot reservation assignment made"""
        self.delete_assignment(assignment_id=self.last_assignment)
        
    def delete_last_reservation(self):
        """Delete the last slot reservation made"""
        self.delete_reservation(reservation_id=self.last_reservation)
        
    def delete_last_commitment(self):
        """Delete the last slot purchase commitment"""
        self.delete_commitment(commitment_id=self.last_commitment)
        
    def delete_all(self):
        """Delete the last assignment, reservation and commitment"""
        self.delete_last_assignment()
        self.delete_last_reservation()
        self.delete_last_commitment()
