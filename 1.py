def run_multithread_batches(fetch_func, ids, region, filters, batch_size=50, max_workers=10, delay_between_batches=0.5):
    start_time = time.time()
    all_results = []

    def process_batch(batch_ids):
        try:
            return fetch_func(batch_ids, region, filters)
        except Exception as e:
            print(f"[ERROR] Batch failed: {e}")
            return None

    batches = [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(process_batch, batch): batch for batch in batches}

        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                result = future.result()
                if result:
                    all_results.append(result)
            except Exception as e:
                print(f"[ERROR] Failed to fetch batch: {e}")

            time.sleep(delay_between_batches)  # throttle API calls

    end_time = time.time()
    print(f"[INFO] Completed {len(batches)} batches in {end_time - start_time:.2f} seconds.")
    return all_results
