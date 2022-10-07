def run_connected_components_test():
    data = "./test_data/blobs.tiff"
    workflow = {0: {"connected_components", None}}
    tile_arrangement = "3,3,1"
    execution_config = "./test_data/slurm_execution_config.json"

    from main import main
    main(data, workflow, tile_arrangement, execution_config)


def run_guassian_filter_test():
    data = "./test_data/blobs.tiff"
    props = {"sigma": 10.0}
    workflow = {0: {"gaussian_filter": props}}
    tile_arrangement = "3,3,1"
    execution_config = "./test_data/slurm_execution_config.json"

    from main import main
    main(data, workflow, tile_arrangement, execution_config)
