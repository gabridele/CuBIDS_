"""Methods for validating BIDS datasets."""

import glob
import json
import logging
import os
import pathlib
import subprocess

import pandas as pd

logger = logging.getLogger("cubids-cli")


def build_validator_call(path, ignore_headers=False):
    """Build a subprocess command to the bids validator."""
    # New schema BIDS validator doesn't have option to ignore subject consistency.
    # Build the deno command to run the BIDS validator.
    command = ["deno", "run", "-A", "jsr:@bids/validator", path, "--verbose", "--json"]

    if ignore_headers:
        command.append("--ignoreNiftiHeaders")

    return command


def build_subject_paths(bids_dir):
    """Build a list of BIDS dirs with 1 subject each."""
    bids_dir = str(bids_dir)
    if not bids_dir.endswith("/"):
        bids_dir += "/"

    root_files = [x for x in glob.glob(bids_dir + "*") if os.path.isfile(x)]

    bids_dir += "sub-*/"

    subjects = glob.glob(bids_dir)

    if len(subjects) < 1:
        raise ValueError("Couldn't find any subjects in the specified directory:\n" + bids_dir)

    subjects_dict = {}

    for sub in subjects:
        purepath = pathlib.PurePath(sub)
        sub_label = purepath.name

        files = [x for x in glob.glob(sub + "**", recursive=True) if os.path.isfile(x)]
        files.extend(root_files)
        subjects_dict[sub_label] = files

    return subjects_dict


def run_validator(call):
    """Run the validator with subprocess.

    Parameters
    ----------
    call : :obj:`list`
        List of strings to pass to subprocess.run().

    Returns
    -------
    :obj:`subprocess.CompletedProcess`
        The result of the subprocess call.
    """
    # if verbose:
    #     logger.info("Running the validator with call:")
    #     logger.info('\"' + ' '.join(call) + '\"')

    ret = subprocess.run(call, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return ret

def parse_issue(issue):
    return {
	"code": issue.get("code", ""),
        "severity": issue.get("severity", ""),
        "location": issue.get("location", ""),
        "affects": ", ".join(issue.get("affects", [])),
        "rule": issue.get("rule", "")
    }   
        
def parse_validator_output(output):
    """Parse the JSON output of the BIDS validator into a pandas dataframe.

    Parameters
    ----------
    output : :obj:`str`
        Path to JSON file of BIDS validator output

    Returns
    -------
    df : :obj:`pandas.DataFrame`
        Dataframe of validator output.
    """
    try:
        data = json.loads(output)

        # Access 'issues' field from the outermost part of the structure
        issues = data.get("issues", {})

        df = pd.DataFrame()

        # Actual issues (errors and warnings) are inside 'issues' -> 'issues'
        issues_list = issues.get("issues", [])

        # Iterate through list of issues
        for issue in issues_list:
            # If issue is an error
            if issue.get("severity") == "error":
                parsed = parse_issue(issue)
                parsed_df = pd.DataFrame([parsed])
                df = pd.concat([df, parsed_df], ignore_index=True)
            # If issue is a warning
            elif issue.get("severity") == "warning":
                parsed = parse_issue(issue)
                parsed_df = pd.DataFrame([parsed])
                df = pd.concat([df, parsed_df], ignore_index=True)

        return df

    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")

def get_val_dictionary():
    """Get value dictionary.

    Returns
    -------
    val_dict : dict
        Dictionary of values.
    """
    return {
        "location": {"Description": "File with the validation issue."},
        "code": {"Description": "Code of the validation issue."},
        "subCode": {"Description": "Subcode providing additional issue details."},
        "severity": {"Description": "Severity of the issue (e.g., warning, error)."},
        "rule": {"Description": "Validation rule that triggered the issue."},
    }
