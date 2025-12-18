#pragma once

#include <vector>
#include <string>
#include <cmath>
#include <algorithm>
#include <iostream>
#include <cstdint>
#include <limits>
#include <tuple>
#include <chrono>

namespace sindex {

struct Segment {
    std::string end_key;
    double slope;
    double intercept;
};

struct PLRResult {
    uint32_t error_bound;
    std::vector<Segment> segments;
    size_t common_prefix_len;
};

class GreedyPLR {
    struct Point {
        double x;
        double y;
    };

    // Helper to convert string to double (using first 8 bytes after offset)
    // This preserves the order for strings that differ in the first 8 bytes.
    static double str_to_double(const std::string& key) {
        double val = 0.0;
        for (char c : key) {
            val = val * 10.0 + (c - '0');
        }
        return val;
    }

public:
    // Builds a Piecewise Linear Regression model for the given data.
    // The algorithm is greedy: it extends the current segment as much as possible
    // while satisfying the error_bound.
    // Returns a PLRResult containing segments and the common prefix length used.
    PLRResult build(uint32_t error_bound) {
        std::vector<Segment> segments;
        if (data.empty()) return {0, segments, 0};

    // auto st = std::chrono::high_resolution_clock::now();

        size_t common_prefix_len = prefix_len_;

    // auto end = std::chrono::high_resolution_clock::now();
    // auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - st).count();

    // std::cout << "get common cost: " << duration_us/1000 << " ms" << std::endl;

        // std::vector<Point> points;
        // points.reserve(data.size());
        // for (const auto& p : data) {
        //     points.push_back({str_to_double(p.first), static_cast<double>(p.second)});
        //     std::cout<<"data key:"<<p.first<<" value:"<<p.second<<std::endl;
        // }

        size_t n = points.size();
        size_t start = 0;

        while (start < n) {
            auto [slope, intercept, next_start] = find_longest_segment(points, start, error_bound);
            
            // Save the segment
            // Note: The segment is valid for [start, next_start - 1]
            segments.push_back({data[next_start - 1].first, slope, intercept});
            
            start = next_start;
        }
        
        //std::cout<<segments.size()<<" segments created with common prefix length "<<common_prefix_len<<std::endl;

        return {error_bound, segments, common_prefix_len};
    }

    static std::pair<size_t, size_t> GetSearchRange(const std::string& key, size_t common_prefix_len, double slope, double intercept, uint32_t error_bound) {
        double x = str_to_double(key);
        double y = slope * x + intercept;
        long long min_pos = static_cast<long long>(y) - error_bound;
        long long max_pos = static_cast<long long>(y) + error_bound;
        if (min_pos < 0) min_pos = 0;
        //std::cout<<"Search range for key "<<key<<" x:"<<x<<": ["<<min_pos<<", "<<max_pos<<"]"<<std::endl;
        return {static_cast<size_t>(min_pos), static_cast<size_t>(max_pos)};
    }

    void PushToData(const std::string& key, const uint32_t value) {
        data.emplace_back(std::move(key), value);
        points.push_back({str_to_double(key), static_cast<double>(value)});
    }

private:

    bool first_ = true;
    std::string base_key_;
    size_t prefix_len_ = 0;

    std::vector<std::pair<std::string, uint32_t>> data;
    std::vector<Point> points;

    // Finds the longest segment starting at 'start' that satisfies the error bound.
    // Returns {slope, intercept, next_start_index}
    std::tuple<double, double, size_t> find_longest_segment(const std::vector<Point>& points, size_t start, double err) {
        size_t n = points.size();
        if (start >= n) return {0, 0, n};
        
        // Single point case
        if (start == n - 1) {
            return {0, points[start].y, n};
        }

        // We use a simplified "Fan" algorithm which is O(N) and robust.
        // It finds a line passing through the "start" region that covers maximum points.
        
        // Let's implement the "Simple Greedy" (Fan from first point):
        // We assume the line passes through (x0, y0).
        // Then for each point i:
        // slope <= (y_i + err - y0) / (x_i - x0)
        // slope >= (y_i - err - y0) / (x_i - x0)
        // We intersect these ranges. If empty, break.
        
        double x0 = points[start].x;
        double y0 = points[start].y;
        
        // Range of valid slopes for line passing through (x0, y0)
        double min_slope = -std::numeric_limits<double>::infinity();
        double max_slope = std::numeric_limits<double>::infinity();
        
        size_t j = start + 1;
        for (; j < n; ++j) {
            double dx = points[j].x - x0;
            if (dx == 0) {
                // Same key (after prefix stripping and 8-byte truncation).
                // If y is within error bound of y0, we can continue (slope doesn't matter for dx=0 if we consider it same point).
                // But if y is different, we have a collision with different values -> impossible to fit line with finite slope?
                // Actually, if dx=0, we have a vertical line segment.
                // A function y = mx + c cannot model a vertical line.
                // However, if |y - y0| <= err, then any slope works for this point relative to (x0, y0) if we consider it "close enough".
                // But strictly, if dx=0, we can't divide by it.
                // If multiple keys map to same double, we can only support them if their values are close enough.
                // If they are not close enough, we must break the segment.
                if (std::abs(points[j].y - y0) > err) {
                    break; 
                }
                continue; 
            }
            
            double y = points[j].y;
            
            // y - err <= m*dx + y0 <= y + err
            // m*dx >= y - err - y0  => m >= (y - y0 - err) / dx
            // m*dx <= y + err - y0  => m <= (y - y0 + err) / dx
            
            double s_min = (y - y0 - err) / dx;
            double s_max = (y - y0 + err) / dx;
            
            min_slope = std::max(min_slope, s_min);
            max_slope = std::min(max_slope, s_max);
            
            if (min_slope > max_slope) {
                // No valid line passing through (x0, y0) covers this point.
                // Break and start new segment.
                break;
            }
        }
        
        // Calculate final slope and intercept
        double final_slope = (min_slope + max_slope) / 2.0;
        if (min_slope == -std::numeric_limits<double>::infinity()) final_slope = 0; // Should not happen
        
        double final_intercept = y0 - final_slope * x0;
        
        return {final_slope, final_intercept, j};
    }
};

} // namespace sindex
