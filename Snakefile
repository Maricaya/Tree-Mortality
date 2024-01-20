import os.path as op

configfile: "config/snakemake.yml"

projdir = op.join(config['root_dir'], config['projections_subdir'])


rule all_projections:
    input:
        expand(
            op.join(projdir, '{model}', '{scenario}.zarr'),
            model=config['projection_models'],
            scenario=config['projection_scenarios'],
        )


rule convert_projection:
    input:
        op.join(projdir, '{model}', '{scenario}')
    output:
        directory(op.join(projdir, '{model}', '{scenario}.zarr'))
    params:
        config['bcm_config']
    script:
        "python src/convert_projections.py {input} {params} {output}"
