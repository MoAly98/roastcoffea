import awkward as ak
import skhep_testdata
import uproot
from coffea import processor
from coffea.nanoevents import NanoAODSchema, NanoEventsFactory

NanoAODSchema.warn_missing_crossrefs = False


def analyze_with_branches(events, num_branches: int, materialize: bool = False) -> dict:
    """Analyze events reading specified number of branches.

    Args:
        events: NanoEvents object
        num_branches: Number of branches to access
        materialize: If True, explicitly materialize arrays with ak.materialize()
    """
    results = {"nevents": len(events)}

    if num_branches >= 1:
        jet_pt = events.Jet.pt
        if materialize:
            jet_pt = ak.materialize(jet_pt)
        results["njets"] = ak.sum(ak.num(jet_pt))

    if num_branches >= 2:
        jet_eta = events.Jet.eta
        if materialize:
            jet_eta = ak.materialize(jet_eta)
        results["mean_jet_eta"] = ak.mean(jet_eta) if len(jet_eta) > 0 else 0

    if num_branches >= 3:
        jet_phi = events.Jet.phi
        if materialize:
            jet_phi = ak.materialize(jet_phi)
        results["mean_jet_phi"] = ak.mean(jet_phi) if len(jet_phi) > 0 else 0

    if num_branches >= 5:
        electron_pt = events.Electron.pt
        electron_eta = events.Electron.eta
        if materialize:
            electron_pt = ak.materialize(electron_pt)
            electron_eta = ak.materialize(electron_eta)
        results["nelectrons"] = ak.sum(ak.num(electron_pt))
        results["mean_electron_eta"] = (
            ak.mean(electron_eta) if len(electron_eta) > 0 else 0
        )

    if num_branches >= 10:
        muon_pt = events.Muon.pt
        muon_eta = events.Muon.eta
        met_pt = events.MET.pt
        met_phi = events.MET.phi
        if materialize:
            muon_pt = ak.materialize(muon_pt)
            muon_eta = ak.materialize(muon_eta)
            met_pt = ak.materialize(met_pt)
            met_phi = ak.materialize(met_phi)
        results["nmuons"] = ak.sum(ak.num(muon_pt))
        results["mean_muon_eta"] = ak.mean(muon_eta) if len(muon_eta) > 0 else 0
        results["mean_met_pt"] = ak.mean(met_pt)
        results["mean_met_phi"] = ak.mean(met_phi)

    return results


def test_uproot_direct(
    filename: str, treepath: str, num_branches: int, materialize: bool = False
) -> int:
    """Test with direct uproot access.

    Args:
        filename: Path to ROOT file
        treepath: Tree name
        num_branches: Number of branches to access
        materialize: If True, explicitly materialize arrays
    """
    f = uproot.open(filename)
    events = NanoEventsFactory.from_root(
        f, treepath=treepath, schemaclass=NanoAODSchema, mode="virtual"
    ).events()

    # Run analysis
    _ = analyze_with_branches(events, num_branches, materialize=materialize)

    # Get bytes read
    bytes_read = f.file.source.num_requested_bytes
    f.close()

    return bytes_read


class BranchTestProcessor(processor.ProcessorABC):
    """Coffea processor for testing."""

    def __init__(self, num_branches: int):
        self.num_branches = num_branches

    def process(self, events):
        return analyze_with_branches(events, self.num_branches)

    def postprocess(self, accumulator):
        return accumulator


def test_coffea(filename: str, num_branches: int) -> int:
    """Test with Coffea processor."""
    fileset = {
        "test": {
            "files": {filename: "Events"},
        },
    }

    proc = BranchTestProcessor(num_branches=num_branches)
    executor = processor.IterativeExecutor()
    runner = processor.Runner(
        executor=executor,
        savemetrics=True,
        schema=NanoAODSchema,
    )

    _, report = runner(
        fileset,
        treename="Events",
        processor_instance=proc,
    )

    return report["bytesread"]


def main():
    """Run comparison tests."""
    # Get test file
    test_file = skhep_testdata.data_path("nanoAOD_2015_CMS_Open_Data_ttbar.root")
    treepath = "Events"

    branch_counts = [1, 2, 3, 5, 10]

    print("=" * 80)
    print("Testing bytesread sensitivity to number of branches accessed")
    print("=" * 80)
    print()

    results = []

    for num_branches in branch_counts:
        print(f"Testing with {num_branches} branches...")

        # Test uproot direct (without materialization)
        uproot_bytes = test_uproot_direct(
            test_file, treepath, num_branches, materialize=False
        )

        # Test uproot direct (with explicit materialization)
        uproot_bytes_mat = test_uproot_direct(
            test_file, treepath, num_branches, materialize=True
        )

        # Test Coffea
        coffea_bytes = test_coffea(test_file, num_branches)

        results.append(
            {
                "branches": num_branches,
                "uproot_kb": uproot_bytes / 1e3,
                "uproot_mat_kb": uproot_bytes_mat / 1e3,
                "coffea_kb": coffea_bytes / 1e3,
            }
        )

        print(f"  Uproot (lazy):        {uproot_bytes / 1e3:8.2f} KB")
        print(f"  Uproot (materialize): {uproot_bytes_mat / 1e3:8.2f} KB")
        print(f"  Coffea:               {coffea_bytes / 1e3:8.2f} KB")
        print()


if __name__ == "__main__":
    main()
