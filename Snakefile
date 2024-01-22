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


rule merge_projection:
    input:
        expand(
            op.join(
                projdir, '{{model}}', '{{scenario}}',
                '{var}_{{model}}_{{scenario}}.nc4'
            ),
            var=config['bcm_variables'],
        )
    output:
        directory(op.join(projdir, '{model}', '{scenario}.zarr'))
    shell:
        "python src/merge_projections.py {input} {output}"


rule convert_projection:
    input:
        op.join(
            projdir, '{model}', '{scenario}',
            '{var}_{model}_{scenario}.zip'
        )
    output:
        op.join(
            projdir, '{model}', '{scenario}',
            '{var}_{model}_{scenario}.nc4'
        )
    params:
        config['bcm_config']
    shell:
        "python src/convert_projections.py {input} {params} {output}"
