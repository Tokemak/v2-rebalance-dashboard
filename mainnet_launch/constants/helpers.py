import time
import line_profiler
import os


def time_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        print(f"{func.__name__} took {elapsed_time:.4f} seconds.")
        return result

    return wrapper


def profile_function(func, *args, **kwargs):
    """
    Profiles the given function line-by-line, prints the result in seconds,
    and saves it to profiles/<function_name>.txt, including total time in seconds.

    Args:
        func (callable): The function to profile.
        *args, **kwargs: Arguments to pass to the function.
    """
    output_dir = "profiles"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{func.__name__}.txt")

    profiler = line_profiler.LineProfiler()
    profiler.add_function(func)
    profiler.enable_by_count()

    try:
        func(*args, **kwargs)
    finally:
        profiler.disable_by_count()

        # Print to console
        profiler.print_stats(output_unit=1)

        # Get total time in seconds
        stats = profiler.get_stats()
        total_time_sec = sum(t for _, rows in stats.timings.items() for _, _, t in rows) * stats.unit

        # Save to file with total time header
        with open(output_path, "w") as f:
            f.write(f"Total time: {total_time_sec:.6f} s\n\n")
            profiler.print_stats(stream=f, output_unit=1)

        print(f"Profile written to: {output_path}")
