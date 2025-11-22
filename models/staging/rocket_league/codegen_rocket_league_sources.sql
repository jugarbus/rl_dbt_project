-- Para generar de forma automáticas los sources con sus campos para rellenar (te hace la estructura)

{{
    codegen.generate_source(
        schema_name = 'rocket_league',
        database_name = 'DEV_BRONZE',
        table_names = ['raw_games_players'],
        generate_columns = True,
        include_descriptions=True,
        include_data_types=True,
        name='desarrollo',
        include_database=True,
        include_schema=True
        )
}}