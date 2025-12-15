// #include "sindex/greedy_plr.h"
// #include <iostream>
// #include <vector>
// #include <string>

// int main() {
//     std::vector<std::pair<std::string, uint32_t>> data = {
//         {"apple1", 10},
//         {"apple2", 20},
//         {"apple3", 30}
//     };
    
//     auto result = sindex::GreedyPLR::build(data, 5);
    
//     std::cout << "Common prefix len: " << result.common_prefix_len << std::endl;
//     std::cout << "Segments: " << result.segments.size() << std::endl;
    
//     for (const auto& seg : result.segments) {
//         std::cout << "Start: " << seg.start_key << ", Slope: " << seg.slope << ", Intercept: " << seg.intercept << std::endl;
//     }
    
//     return 0;
// }
